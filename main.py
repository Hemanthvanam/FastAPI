from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pyodbc
import google.generativeai as genai
import pandas as pd
import re

# --- CONFIGURE GEMINI ---
genai.configure(api_key="AIzaSyAayWiTM2snxqs2P13daKGkf87LshVPSeQ")
model = genai.GenerativeModel("gemini-1.5-flash")

# --- DATABASE CONNECTION STRING ---
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=tcp:q2jdkkazwpdufgw7l5isv2yegu-ulyatwdewx3ufjannhm2hew6yi.datawarehouse.fabric.microsoft.com,1433;"
    "DATABASE=bing_lake_db;"
    "Authentication=ActiveDirectoryInteractive;"
    "UID=vanam.hemanth@isteer.com;"
)

# --- TABLE SCHEMA FOR PROMPTING ---
TABLE_SCHEMA = """
Table: tbl_sentiment_analysis
Columns:
- title (Text)
- description (Text)
- category (Text)
- url (URL)
- image (Text)
- provider (Text)
- datePublished (Date)
- sentiment (Text): values are "positive", "negative", or "neutral"
"""

# --- FASTAPI SETUP ---
app = FastAPI()

# --- ENABLE CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to specific domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- REQUEST MODEL ---
class ChatRequest(BaseModel):
    prompt: str

# --- HELPERS ---
def extract_sql_only(generated_text: str) -> str:
    match = re.search(r"(SELECT[\s\S]+?;)", generated_text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"(SELECT[\s\S]+)", generated_text, re.IGNORECASE)
    return match.group(1).strip() if match else generated_text.strip()

def run_sql(query: str) -> dict:
    try:
        with pyodbc.connect(CONNECTION_STRING) as conn:
            df = pd.read_sql(query, conn)
        return {
            "type": "sql_result",
            "columns": list(df.columns),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        return {"type": "error", "message": f"SQL execution failed: {e}"}

def is_dax_request(prompt: str) -> bool:
    return "dax" in prompt.lower() or "write dax" in prompt.lower()

def is_data_query(prompt: str) -> bool:
    keywords = ['show', 'list', 'news', 'sentiment', 'category', 'published', 'description', 'records', 'count']
    return any(word in prompt.lower() for word in keywords)

def generate_dax(prompt: str) -> str:
    full_prompt = f"""
    Based on this table schema, write a DAX expression: {prompt}
    {TABLE_SCHEMA}
    """
    response = model.generate_content(full_prompt)
    return response.text.strip()

def handle_data_query(prompt: str) -> dict:
    # Handle special case
    if "how many records" in prompt.lower() and "today" in prompt.lower():
        query = """
            SELECT COUNT(*) AS RecordsIngestedToday
            FROM tbl_sentiment_analysis
            WHERE datePublished >= CAST(GETDATE() AS DATE)
              AND datePublished < DATEADD(day, 1, CAST(GETDATE() AS DATE));
        """
        return run_sql(query)

    full_prompt = f"""
You are an expert in SQL Server.
Write a valid SQL Server query for the following request based on table 'tbl_sentiment_analysis'.
Use SQL Server syntax only (e.g., FORMAT() for date formatting).
Table schema:
{TABLE_SCHEMA}

Request: {prompt}
"""
    response = model.generate_content(full_prompt)
    sql_raw = response.text
    sql_query = extract_sql_only(sql_raw)

    # Fix Gemini datePublished for SQL Server
    sql_query = re.sub(
        r"datePublished\s*=\s*CONVERT\(DATE,\s*GETDATE\(\)\)",
        "CAST(datePublished AS DATE) = CAST(GETDATE() AS DATE)",
        sql_query,
        flags=re.IGNORECASE
    )
    sql_query = re.sub(
        r"datePublished\s*=\s*CAST\(GETDATE\(\)\s+AS\s+DATE\)",
        "CAST(datePublished AS DATE) = CAST(GETDATE() AS DATE)",
        sql_query,
        flags=re.IGNORECASE
    )

    return run_sql(sql_query)

# --- MAIN ENDPOINT ---
@app.post("/chat")
async def chat(request: ChatRequest):
    prompt = request.prompt.strip()
    try:
        if is_dax_request(prompt):
            dax = generate_dax(prompt)
            return {"type": "dax", "response": dax}

        elif is_data_query(prompt):
            result = handle_data_query(prompt)
            return result

        else:
            # General Gemini conversation
            response = model.generate_content(prompt)
            return {"type": "text", "response": response.text.strip()}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
