import pandas as pd
from pandas.api.types import is_datetime64tz_dtype
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import pytz
import traceback

# --- CONFIGURAÇÕES ---
DB_URL = os.environ.get('DATABASE_URL', 'postgresql+psycopg2://postgres:2025@localhost:5432/pedidos_db')
engine = create_engine(DB_URL)
FUSO_BRASILIA = pytz.timezone("America/Sao_Paulo")
META_SEMANAL = 500

def to_safe_dict(df):
    if df.empty:
        return []
    return df.astype(object).where(pd.notnull(df), None).to_dict('records')

def _to_naive_series_for_grouping(ts: pd.Series) -> pd.Series:
    """
    Garante que a série de timestamps seja naive (sem tz) para operações
    de groupby por período, de maneira segura.
    """
    if ts.empty:
        return ts
    if is_datetime64tz_dtype(ts):
        # converte para UTC e depois remove tzinfo
        return ts.dt.tz_convert('UTC').dt.tz_localize(None)
    return ts

def get_painel_data():
    """
    Função principal que busca e organiza todos os dados para a API do painel.
    Agora, esta função gere a sua própria ligação ao banco.
    """
    query = text("""
        SELECT 
            p.id, p.status_id, p.equipamento, p.pv, p.descricao_servico,
            s.nome_status, p.data_criacao, p.quantidade, p.urgente,
            p.data_conclusao, i.nome as nome_imagem, p.prioridade,
            p.tem_office, hs.data_mudanca as ultima_mudanca
        FROM 
            pedidos_tb p
        LEFT JOIN
            status_td s ON p.status_id = s.id
        LEFT JOIN                               
            imagem_td i ON p.imagem_id = i.id
        LEFT JOIN (
		    SELECT 
		        pedido_id,
		        MAX(data_mudanca) AS data_mudanca
		    FROM historico_status_tb
		    GROUP BY pedido_id
		) AS hs ON hs.pedido_id = p.id
        ORDER BY
            p.urgente DESC, p.prioridade ASC;
    """)

    dados_vazios = {
        "prioridades": [],
        "backlog": {"lista": [], "total": 0},
        "aguardando": {"lista": [], "total": 0},
        "pendentes": {"lista": [], "total": 0},
        "em_montagem_fora_prioridade": {"lista": [], "total": 0},
        "concluidos_hoje": {"lista": [], "total_pedidos": 0, "total_maquinas": 0},
        "cancelados_hoje": {"lista": [], "total_pedidos": 0, "total_maquinas": 0},
        "metricas": {
            "total_mes_pedidos": 0,
            "total_mes_maquinas": 0,
            "media_diaria_pedidos": 0.0,
            "media_diaria_maquinas": 0.0,
            "recorde_dia_data": "N/A",
            "recorde_dia_pedidos": 0,
            "recorde_dia_maquinas": 0
        },
        "desempenho_semanal": [],
        "meta_semanal": META_SEMANAL
    }

    try:
        # --- GESTÃO DE LIGAÇÃO MELHORADA ---
        with engine.connect() as conn:
            df_full = pd.read_sql(query, conn)

        if df_full.empty:
            return dados_vazios

        # --- Processamento e Limpeza de Dados ---
        df_full.fillna({
            'nome_status': 'Não Definido',
            'pv': 'N/A',
            'equipamento': 'Não informado',
            'descricao_servico': 'N/A',
            'nome_imagem': 'N/A',
            'urgente': False,
            'tem_office': False
        }, inplace=True)
        # evita o FutureWarning sobre downcasting automático
        df_full.infer_objects(copy=False)

        df_full['quantidade'] = pd.to_numeric(df_full['quantidade'], errors='coerce').fillna(0).astype(int)
        df_full['prioridade'] = pd.to_numeric(df_full['prioridade'], errors='coerce').fillna(9999).astype(int)

        # parse de datas (retorna tz-aware em UTC)
        df_full['data_criacao'] = pd.to_datetime(df_full['data_criacao'], errors='coerce', utc=True)
        df_full['data_conclusao'] = pd.to_datetime(df_full['data_conclusao'], errors='coerce', utc=True)

        # converte para fuso de Brasilia apenas se não for tudo nulo
        if not df_full['data_criacao'].isnull().all():
            df_full.loc[df_full['data_criacao'].notna(), 'data_criacao'] = \
                df_full.loc[df_full['data_criacao'].notna(), 'data_criacao'].dt.tz_convert(FUSO_BRASILIA)
        if not df_full['data_conclusao'].isnull().all():
            df_full.loc[df_full['data_conclusao'].notna(), 'data_conclusao'] = \
                df_full.loc[df_full['data_conclusao'].notna(), 'data_conclusao'].dt.tz_convert(FUSO_BRASILIA)

        # --- Lógica de Negócio (cálculos) ---
        STATUS_ID_CONCLUIDO, STATUS_ID_CANCELADO, STATUS_ID_PENDENTE, STATUS_ID_BACKLOG, STATUS_ID_AGUARDANDO, STATUS_ID_MONTAGEM, STATUS_ID_FINALIZARWMS= 4, 6, 5, 2, 1, 3, 0

        hoje = datetime.now(FUSO_BRASILIA)
        inicio_do_dia = hoje.replace(hour=0, minute=0, second=0, microsecond=0)
        inicio_mes_atual = hoje.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        df_em_andamento = df_full[~df_full['status_id'].isin([STATUS_ID_CONCLUIDO, STATUS_ID_CANCELADO, STATUS_ID_FINALIZARWMS])].copy()

        prioridades_df = df_em_andamento[~df_em_andamento['status_id'].isin([STATUS_ID_AGUARDANDO, STATUS_ID_PENDENTE])].head(4).copy()
        prioridades_ids = set(prioridades_df['id'].tolist())

        montagem_fora_df = df_em_andamento[
            (df_em_andamento['status_id'] == STATUS_ID_MONTAGEM) & (~df_em_andamento['id'].isin(prioridades_ids))
        ].copy()

        backlog_df = df_em_andamento[df_em_andamento['status_id'] == STATUS_ID_BACKLOG].copy()
        aguardando_df = df_em_andamento[df_em_andamento['status_id'] == STATUS_ID_AGUARDANDO].copy()
        pendentes_df = df_em_andamento[df_em_andamento['status_id'] == STATUS_ID_PENDENTE].copy()

        # --- Correção dos warnings: criar máscaras combinadas sobre df_full (mesmo índice) ---
        mask_data_conclusao_notnull = df_full['data_conclusao'].notna()
        mask_concluidos_hoje = mask_data_conclusao_notnull & (df_full['data_conclusao'] >= inicio_do_dia)
        df_finalizados_hoje = df_full[mask_concluidos_hoje].copy()

        concluidos_hoje_df = df_finalizados_hoje[df_finalizados_hoje['status_id'] == STATUS_ID_CONCLUIDO].copy()
        cancelados_hoje_df = df_finalizados_hoje[df_finalizados_hoje['status_id'] == STATUS_ID_CANCELADO].copy()

        mask_concluidos_mes = mask_data_conclusao_notnull & (df_full['status_id'] == STATUS_ID_CONCLUIDO) & (df_full['data_conclusao'] >= inicio_mes_atual)
        df_concluidos_mes_atual = df_full[mask_concluidos_mes].copy()

        total_mes_pedidos = len(df_concluidos_mes_atual)
        total_mes_maquinas = int(df_concluidos_mes_atual['quantidade'].sum()) if total_mes_pedidos > 0 else 0
        media_diaria_pedidos = total_mes_pedidos / hoje.day if hoje.day > 0 else 0
        media_diaria_maquinas = total_mes_maquinas / hoje.day if hoje.day > 0 else 0

        recorde_dia_data, recorde_dia_pedidos, recorde_dia_maquinas = "N/A", 0, 0
        if not df_concluidos_mes_atual.empty:
            # agrupar por dia (usar versão naive da timestamp para evitar problemas com tz)
            ts_naive = _to_naive_series_for_grouping(df_concluidos_mes_atual['data_conclusao'])
            recorde_dia_series = df_concluidos_mes_atual.groupby(ts_naive.dt.date)['quantidade'].sum()
            if not recorde_dia_series.empty:
                recorde_dia = recorde_dia_series.idxmax()
                df_recorde = df_concluidos_mes_atual[ts_naive.dt.date == recorde_dia]
                recorde_dia_data = recorde_dia.strftime('%d/%m/%Y')
                recorde_dia_pedidos = len(df_recorde)
                recorde_dia_maquinas = int(df_recorde['quantidade'].sum())

        # --- Desempenho semanal (últimas 4 semanas) ---
        df_concluidos_full = df_full[mask_data_conclusao_notnull & (df_full['status_id'] == STATUS_ID_CONCLUIDO)].copy()
        desempenho_semanal_list = []
        if not df_concluidos_full.empty:
            ts_naive_full = _to_naive_series_for_grouping(df_concluidos_full['data_conclusao'])
            semana_inicio = ts_naive_full.dt.to_period('W-MON').apply(lambda p: p.start_time.date())
            desempenho_semanal = df_concluidos_full.groupby(semana_inicio)['quantidade'].sum().tail(4)
            desempenho_semanal_list = [{"semana": k.strftime('%Y-%m-%d'), "valor": int(v)} for k, v in desempenho_semanal.items()]

        return {
            "prioridades": to_safe_dict(prioridades_df),
            "backlog": {"lista": to_safe_dict(backlog_df.head(5)), "total": len(backlog_df)},
            "aguardando": {"lista": to_safe_dict(aguardando_df.head(5)), "total": len(aguardando_df)},
            "pendentes": {"lista": to_safe_dict(pendentes_df.head(5)), "total": len(pendentes_df)},
            "em_montagem_fora_prioridade": {"lista": to_safe_dict(montagem_fora_df.head(5)), "total": len(montagem_fora_df)},
            "concluidos_hoje": {
                "lista": to_safe_dict(concluidos_hoje_df),
                "total_pedidos": len(concluidos_hoje_df),
                "total_maquinas": int(concluidos_hoje_df['quantidade'].sum()) if len(concluidos_hoje_df) > 0 else 0
            },
            "cancelados_hoje": {
                "lista": to_safe_dict(cancelados_hoje_df),
                "total_pedidos": len(cancelados_hoje_df),
                "total_maquinas": int(cancelados_hoje_df['quantidade'].sum()) if len(cancelados_hoje_df) > 0 else 0
            },
            "metricas": {
                "total_mes_pedidos": total_mes_pedidos,
                "total_mes_maquinas": total_mes_maquinas,
                "media_diaria_pedidos": round(media_diaria_pedidos, 1),
                "media_diaria_maquinas": round(media_diaria_maquinas, 1),
                "recorde_dia_data": recorde_dia_data,
                "recorde_dia_pedidos": recorde_dia_pedidos,
                "recorde_dia_maquinas": recorde_dia_maquinas
            },
            "desempenho_semanal": desempenho_semanal_list,
            "meta_semanal": META_SEMANAL
        }

    except Exception as e:
        # Log completo para depuração no console
        print(f"ERRO CRÍTICO ao processar dados para o painel: {e}")
        traceback.print_exc()
        # retorna um dict consistente com o formato esperado (não tuple)
        return {"error": "Falha ao processar os dados.", "details": str(e)}
