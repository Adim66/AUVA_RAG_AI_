from pathlib import Path
from typing import Optional

def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract text from PDF using pdfplumber"""
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            text = '\n'.join(page.extract_text() for page in pdf.pages if page.extract_text())
        return text
    except ImportError:
        raise ImportError("pdfplumber not installed. Run: pip install pdfplumber")

def extract_text_from_txt(txt_path: str) -> str:
    """Extract text from plain text file"""
    with open(txt_path, 'r', encoding='utf-8') as f:
        return f.read()

def extract_text(file_path: str) -> str:
    """Extract text from file based on extension"""
    path = Path(file_path)
    if path.suffix.lower() == '.pdf':
        return extract_text_from_pdf(file_path)
    elif path.suffix.lower() in ['.txt', '.md']:
        return extract_text_from_txt(file_path)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")
