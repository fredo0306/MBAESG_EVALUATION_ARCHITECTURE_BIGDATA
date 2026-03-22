import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

st.set_page_config(page_title="Analyse LinkedIn", layout="wide")

session = get_active_session()

# 1. Top 10 des titres de postes par industrie

st.title("Top 10 des titres de postes par industrie")

query = """
SELECT
    ji.industry_id,
    jp.title,
    COUNT(*) AS nb_offres
FROM job_postings jp
JOIN job_industries ji 
    ON jp.job_id = ji.job_id
GROUP BY ji.industry_id, jp.title
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ji.industry_id 
    ORDER BY COUNT(*) DESC
) <= 10
"""

df = session.sql(query).to_pandas()

st.dataframe(df)
st.bar_chart(df, x="TITLE", y="NB_OFFRES")

# 2. Top 10 des postes les mieux rémunérés 
st.title("Top 10 des postes les mieux rémunérés par industrie")

query = """
SELECT
    ji.industry_id,
    jp.title,
    jp.max_salary
FROM job_postings jp
JOIN job_industries ji 
    ON jp.job_id = ji.job_id
WHERE jp.max_salary IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ji.industry_id 
    ORDER BY jp.max_salary DESC
) <= 10
"""

df = session.sql(query).to_pandas()

st.dataframe(df)
st.bar_chart(df, x="TITLE", y="MAX_SALARY")

#3. Répartition par taille d’entreprise
st.title("Répartition des offres par taille d’entreprise")

query = """
SELECT
    company_size,
    COUNT(*) AS nb_offres
FROM companies c
JOIN job_postings jp ON jp.job_id IS NOT NULL -- juste pour lier
GROUP BY company_size
ORDER BY company_size
"""

df = session.sql(query).to_pandas()
st.dataframe(df)
st.bar_chart(df.set_index("COMPANY_SIZE")["NB_OFFRES"])

# 4. Répartition par secteur d’activité
st.title("Répartition des offres par secteur d’activité")

query = """
SELECT
    ji.industry_id AS industry,
    COUNT(*) AS nb_offres
FROM job_industries ji
GROUP BY ji.industry_id
ORDER BY nb_offres DESC
"""

df = session.sql(query).to_pandas()
st.dataframe(df)
st.bar_chart(df.set_index("INDUSTRY")["NB_OFFRES"])
#5. Répartition par type d’emploi

st.title("Répartition des offres par type d’emploi")

query = """
SELECT
    work_type,
    COUNT(*) AS nb_offres
FROM job_postings
GROUP BY work_type
ORDER BY nb_offres DESC
"""

df = session.sql(query).to_pandas()

st.dataframe(df)
st.bar_chart(df, x="WORK_TYPE", y="NB_OFFRES")