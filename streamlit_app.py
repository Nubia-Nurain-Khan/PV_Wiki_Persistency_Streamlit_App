import streamlit as st
import pandas as pd
import requests
from datetime import datetime, date
from io import BytesIO

############
# Global settings
############

date_format = "%Y-%m-%dT%H:%M:%SZ"
default_vox_start = datetime(2014, 2, 28)

# Project Vox-related pages
page_names = {
    "Margaret Cavendish (English)": ("Margaret_Cavendish,_Duchess_of_Newcastle-upon-Tyne", "en"),
    "Margaret Cavendish (French)": ("Margaret_Cavendish", "fr"),
    "Margaret Cavendish (Spanish)": ("Margaret_Cavendish", "es"),
    "Margaret Cavendish (German)": ("Margaret_Cavendish,_Duchess_of_Newcastle", "de"),
    "Margaret Cavendish (Italian)": ("Margaret_Cavendish", "it"),
    "Margaret Cavendish (Russian)": ("Кавендиш,_Маргарет", "ru"),
    "Émilie du Châtelet (English)": ("Émilie_du_Châtelet", "en"),
    "Émilie du Châtelet (German)": ("Émilie_du_Châtelet", "de"),
    "Émilie du Châtelet (Spanish)": ("Émilie_du_Châtelet", "es"),
    "Émilie du Châtelet (French)": ("Émilie_du_Châtelet", "fr"),
    "Émilie du Châtelet (Italian)": ("Émilie_du_Châtelet", "it"),
    "Émilie du Châtelet (Vietnamese)": ("Émilie_du_Châtelet", "vi"),
    "Anne Conway (English)": ("Anne_Conway_(philosopher)", "en"),
    "Anne Conway (Italian)": ("Anne_Conway", "it"),
    "Anne Conway (French)": ("Anne_Conway", "fr"),
    "Anne Finch (Spanish)": ("Anne_Finch", "es"),
    "Anne Conway (German)": ("Anne_Conway", "de"),
    "Anne Conway (Russian)": ("Конуэй,_Энн", "ru"),
    "Anne Conway (Korean)": ("앤_콘웨이", "ko"),
    "Juana Inés de la Cruz (English)": ("Juana_Inés_de_la_Cruz", "en"),
    "Juana Inés de la Cruz (German)": ("Juana_Inés_de_la_Cruz", "de"),
    "Juana Inés de la Cruz (Spanish)": ("Juana_Inés_de_la_Cruz", "es"),
    "Juana Inés de la Cruz (French)": ("Juana_Inés_de_la_Cruz", "fr"),
    "Juana Inés de la Cruz (Italian)": ("Juana_Inés_de_la_Cruz", "it"),
    "Juana Inés de la Cruz (Vietnamese)": ("Juana_Inés_de_la_Cruz", "vi"),
    "Juana Inés de la Cruz (Russian)": ("Хуана_Инес_де_ла_Крус", "ru"),
    "Juana Inés de la Cruz (Korean)": ("후아나_이네스_데_라_크루스", "ko"),
    "Juana Inés de la Cruz (Chinese)": ("胡安娜·伊内斯·德·拉·克鲁兹", "zh"),
}

# Project Vox editors
default_editors = [
    "ZL027", "Nubia Nurain Khan", "NetwonsBucket", "My Poor Meatball",
    "Modernistarthistorian", "MaxyMama", "Lizmilewicz", "Lily Saige",
    "Janiak123", "Fmercer", "Citedesdames", "13mpurcell11",
    "Following Zero", "Bemonubu", "Oddlyintoppe", "lindsaymarie403",
    "ayji", "wsshaw498", "philosophyfan_22", "historian42", "wiki_wanderer",
    "RenaissanceMind", "earlymodernwoman", "feministtheorist",
    "intellectualroots", "academic_writer", "FccFcc", "hlj2014", "Jmbanks23",
    "Aurorakexin", "mjv5712", "Lascano 222", "Truth And Humility Matter", "Sortizhinojosa",
]

###############
# Helper functions
###############

def parse_wiki_url(url: str):
    """Parse a full Wikipedia URL into (title, language)."""
    url = url.strip()
    if "wikipedia.org/wiki/" not in url:
        return None, None
    try:
        # e.g. https://en.wikipedia.org/wiki/Title
        lang = url.split("://")[1].split(".wikipedia.org")[0]
        title = url.split("/wiki/")[1]
        return title, lang
    except Exception:
        return None, None


def fetch_revisions(language: str, title: str, start_dt: datetime | None = None):
    """
    Fetch revisions for a page using the MediaWiki Action API.
    Returns a list of dicts compatible with process_revisions().
    """
    headers = {
        "User-Agent": "ProjectVoxPersistencyTool/1.0 (nubia.khan@duke.edu)"
    }

    session = requests.Session()
    base_url = f"https://{language}.wikipedia.org/w/api.php"

    params = {
        "action": "query",
        "prop": "revisions",
        "titles": title,
        "rvprop": "ids|timestamp|user|userid|size",
        "rvlimit": "max",      # up to 500 per request
        "rvdir": "newer",      # oldest -> newest
        "format": "json",
        "formatversion": "2",
    }

    # Optimization: only fetch from chosen start date onward
    if start_dt is not None:
        params["rvstart"] = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    all_revisions = []
    last_size = None

    while True:
        try:
            r = session.get(base_url, params=params, headers=headers, timeout=30)
            r.raise_for_status()
        except Exception as e:
            st.error(f"API error: {e}")
            break

        data = r.json()
        pages = data.get("query", {}).get("pages", [])
        if not pages:
            break

        revisions = pages[0].get("revisions", [])
        for rev in revisions:
            size = rev.get("size")
            if last_size is None or size is None:
                delta = 0
            else:
                delta = size - last_size
            last_size = size

            all_revisions.append({
                "id": rev["revid"],
                "user": {
                    "name": rev.get("user", ""),
                    "id": rev.get("userid", 0),
                },
                "timestamp": rev["timestamp"],
                "delta": delta,
            })

        cont = data.get("continue", {})
        rvcontinue = cont.get("rvcontinue")
        if rvcontinue:
            params["rvcontinue"] = rvcontinue
        else:
            break

    return all_revisions


def process_revisions(revisions, page_label: str, language: str):
    """Turn raw revisions into a DataFrame."""
    rows = []
    for rev in revisions:
        rows.append({
            "page_label": page_label,
            "language": language,
            "revision_id": rev["id"],
            "user_name": rev["user"]["name"],
            "user_id": rev["user"]["id"],
            "timestamp": rev["timestamp"],
            "delta": rev.get("delta", 0),
        })
    return pd.DataFrame(rows)


def calculate_persistency(df: pd.DataFrame):
    """Compute persistency for each revision in hours & days."""
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], format=date_format)
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Difference to next revision
    df["persistency_seconds"] = (
        df["timestamp"].shift(-1) - df["timestamp"]
    ).dt.total_seconds()

    # Last edit → persists until now (UTC)
    now = datetime.utcnow()
    df.loc[df.index[-1], "persistency_seconds"] = (
        now - df.loc[df.index[-1], "timestamp"]
    ).total_seconds()

    df["persistency_hours"] = df["persistency_seconds"] / 3600
    df["persistency_days"] = df["persistency_seconds"] / 86400
    return df


def filter_data(df: pd.DataFrame, start_dt: datetime, editors_to_include):
    """Filter revisions by date and (optionally) by editor list."""
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], format=date_format)
    df = df[df["timestamp"] >= start_dt]
    if editors_to_include is not None:
        df = df[df["user_name"].isin(editors_to_include)]
    return df


def build_pdf_summary(df, page_label, lang, start_dt):
    """
    Build a simple PDF summary report (in-memory) for download.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    y = height - 72  # top margin

    # Title
    c.setFont("Helvetica-Bold", 14)
    c.drawString(72, y, "Project Vox – Wikipedia Edit Persistency Report")
    y -= 24

    # Basic info
    c.setFont("Helvetica", 11)
    c.drawString(72, y, f"Page: {page_label} ({lang})")
    y -= 16
    c.drawString(72, y, f"Start date filter: {start_dt.date()}")
    y -= 16
    c.drawString(72, y, f"Generated on: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    y -= 24

    # Overview metrics
    total_edits = len(df)
    distinct_editors = df["user_name"].nunique()
    first_edit = df["timestamp"].min()
    last_edit = df["timestamp"].max()

    c.drawString(72, y, f"Total edits (filtered): {total_edits}")
    y -= 16
    c.drawString(72, y, f"Distinct editors: {distinct_editors}")
    y -= 16
    c.drawString(72, y, f"Date range in data: {first_edit.date()} → {last_edit.date()}")
    y -= 24

    # Top editors
    editor_summary = (
        df.groupby("user_name")["persistency_hours"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )

    c.setFont("Helvetica-Bold", 12)
    c.drawString(72, y, "Top editors by mean persistency (hours)")
    y -= 18
    c.setFont("Helvetica", 10)

    for name, val in editor_summary.items():
        line = f"{name}: {val:.2f} h"
        if y < 72:  # new page if we're too low
            c.showPage()
            y = height - 72
            c.setFont("Helvetica", 10)
        c.drawString(72, y, line)
        y -= 14

    c.showPage()
    c.save()
    buffer.seek(0)
    return buffer

##############
# Streamlit App
##############

st.set_page_config(page_title="Project Vox – Wikipedia Persistency", layout="wide")

st.title("Project Vox – Wikipedia Edit Persistency")
st.write("Choose a Project Vox page or paste a Wikipedia URL to see live persistency stats, charts, and downloads.")

# --- Input controls ---

left, right = st.columns([2, 1])

with left:
    mode = st.radio(
        "Input mode",
        ["Pages Edited by Project Vox", "Custom Wikipedia URL"],
        horizontal=True,
    )

    if mode == "Pages Edited by Project Vox":
        chosen_label = st.selectbox("Select a page", list(page_names.keys()))
        title, lang = page_names[chosen_label]
        page_label = chosen_label
    else:
        url = st.text_input(
            "Paste a Wikipedia URL",
            placeholder="https://en.wikipedia.org/wiki/Margaret_Cavendish,_Duchess_of_Newcastle-upon-Tyne",
        )
        title, lang = parse_wiki_url(url) if url else (None, None)
        page_label = url if url else ""

with right:
    # Date filter
    start_date_input = st.date_input(
        "Start date for revisions",
        value=default_vox_start.date()
    )
    start_dt = datetime.combine(start_date_input, datetime.min.time())

    # Editor filter
    editor_mode = st.radio("Editors", ["Project Vox editors only", "All editors"])
    if editor_mode == "Project Vox editors only":
        editors_to_include = default_editors
    else:
        editors_to_include = None

# --- Validate page selection ---

if not title or not lang:
    st.info("Select a Project Vox page or paste a valid Wikipedia URL to begin.")
    st.stop()

st.markdown(f"**Selected page:** `{title}` (language: `{lang}`)")

# --- Fetch & process data ---

with st.spinner("Fetching revision history and calculating persistency…"):
    revisions = fetch_revisions(lang, title, start_dt=start_dt)
    if not revisions:
        st.error("No revisions found for this page or API error occurred.")
        st.stop()

    df_raw = process_revisions(revisions, page_label=page_label, language=lang)
    df_filtered = filter_data(df_raw, start_dt=start_dt, editors_to_include=editors_to_include)

    if df_filtered.empty:
        st.warning("No revisions after applying date/editor filters.")
        st.stop()

    df = calculate_persistency(df_filtered)

# --- Overview metrics ---

st.subheader("Overview")

total_edits = len(df)
distinct_editors = df["user_name"].nunique()
first_edit = df["timestamp"].min()
last_edit = df["timestamp"].max()

m1, m2, m3 = st.columns(3)
m1.metric("Total edits (filtered)", total_edits)
m2.metric("Distinct editors", distinct_editors)
m3.metric("Date range", f"{first_edit.date()} → {last_edit.date()}")

# --- Tabs: charts, table, downloads ---

tab_overview, tab_charts, tab_table, tab_download = st.tabs(
    ["Overview text", "Charts", "Table", "Downloads"]
)

with tab_overview:
    st.markdown(
        f"""
        **Page label:** {page_label or title}  
        **Language:** `{lang}`  
        **Start date filter:** {start_dt.date()}  
        **Editor mode:** {"Project Vox editors only" if editors_to_include is not None else "All editors"}
        """
    )
    st.write("You can switch tabs above to see charts, the full table, and download options.")

with tab_charts:
    st.markdown("### Edits over time")
    df_time = df.set_index("timestamp").sort_index()
    df_time_series = df_time.resample("D")["revision_id"].count()
    st.line_chart(df_time_series)

    st.markdown("### Mean persistency by editor (hours)")
    editor_stats = (
        df.groupby("user_name")["persistency_hours"]
        .mean()
        .sort_values(ascending=False)
    )
    st.bar_chart(editor_stats)

    st.markdown("### Distribution of persistency (days)")
    st.bar_chart(df["persistency_days"])

with tab_table:
    st.markdown("### Revision-level data")
    st.dataframe(
        df[
            [
                "page_label",
                "language",
                "revision_id",
                "user_name",
                "timestamp",
                "delta",
                "persistency_hours",
                "persistency_days",
            ]
        ],
        use_container_width=True,
    )

with tab_download:
    st.markdown("### Download data")

    # CSV download
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV (revisions)",
        data=csv_bytes,
        file_name="persistency_data.csv",
        mime="text/csv",
    )

    # Excel download (revisions + editor summary)
    editor_summary = (
        df.groupby("user_name")
        .agg(
            edits=("revision_id", "count"),
            mean_persistency_hours=("persistency_hours", "mean"),
            total_persistency_hours=("persistency_hours", "sum"),
        )
        .sort_values("edits", ascending=False)
    )

    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Revisions", index=False)
        editor_summary.to_excel(writer, sheet_name="Editor Summary")
    excel_buffer.seek(0)

    st.download_button(
        "Download Excel (revisions + editor summary)",
        data=excel_buffer,
        file_name="persistency_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # PDF summary download
    try:
        pdf_buffer = build_pdf_summary(
            df,
            page_label or title,
            lang,
            start_dt,
        )
        st.download_button(
            "Download PDF summary report",
            data=pdf_buffer,
            file_name="persistency_summary.pdf",
            mime="application/pdf",
        )
    except Exception:
        st.info(
            "PDF report not available. "
            "Make sure the 'reportlab' package is installed (pip install reportlab)."
        )

