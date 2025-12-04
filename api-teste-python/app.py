from flask import Flask, jsonify
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)

DB_USER = "postgres"
DB_PASS = "12345678"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "LeandroIntegracaoDiaria"

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL)

# >>>>>>>>>>>   ROTAS   <<<<<<<<<<<<<<
@app.route("/health", methods=["GET"])
def health():
    """Rota simples para ver se a API está de pé."""
    return jsonify({"status": "ok"})

@app.route("/empresas", methods=["GET"])
def listar_empresas():
    query = "select apelido, razaosocial from cadgw order by apelido LIMIT 100"

    # df de dataframe, container de dados
    df = pd.read_sql(query, engine)

    # convertendo o dataframe para um dicionario
    dados = df.to_dict(orient="records")

    return jsonify(dados)

@app.route("/lancamentos_empresas", methods=["GET"])
def lancamentos_empresas():
    query = "select apelidopg, valor from lctos2"
    df = pd.read_sql(query, engine)

    resumo = (
        df.groupby("apelidopg")["valor"]        
        .sum()
        .reset_index()
        .rename(columns={"valor": "total_lancamentos"})
    )

    return jsonify(resumo.to_dict(orient="records"))

if __name__ == "__main__":
  app.run(debug=True)