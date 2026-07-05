"""Generate test data for CDC testing"""
from pathlib import Path

# Sample news articles
ARTICLES = [
    {
        "id": "article_001",
        "title": "AI Breakthrough in Natural Language Processing",
        "content": """Researchers at leading universities have announced a major breakthrough in natural language processing.
The new model achieves state-of-the-art results on multiple benchmarks.
This advancement could revolutionize how machines understand human language.
Applications include improved chatbots, translation systems, and content generation."""
    },
    {
        "id": "article_002",
        "title": "Climate Change Report Released",
        "content": """A comprehensive climate change report was released today by international scientists.
The report highlights urgent need for action to reduce carbon emissions.
Global temperatures continue to rise at an alarming rate.
Renewable energy adoption is critical for meeting climate goals."""
    },
    {
        "id": "article_003",
        "title": "New Quantum Computing Milestone",
        "content": """Scientists have achieved a new milestone in quantum computing research.
The quantum processor demonstrated quantum advantage over classical computers.
This breakthrough brings us closer to practical quantum applications.
Potential uses include cryptography, drug discovery, and optimization problems."""
    },
    {
        "id": "article_004",
        "title": "Space Exploration Update",
        "content": """NASA announced plans for upcoming Mars missions.
The new rover will search for signs of ancient microbial life.
International collaboration is key to future space exploration.
Private companies are also contributing to space technology development."""
    },
    {
        "id": "article_005",
        "title": "Healthcare Innovation in Telemedicine",
        "content": """Telemedicine adoption has accelerated significantly in recent years.
Patients can now consult doctors remotely using video conferencing.
This technology improves healthcare access in rural areas.
Digital health records enable better coordination of care."""
    }
]

# Modified versions for CDC testing
MODIFIED_ARTICLES = {
    "article_001": """Researchers at leading universities have announced a major breakthrough in natural language processing.
The new model achieves state-of-the-art results on multiple benchmarks.
This advancement could revolutionize how machines understand human language.
Applications include improved chatbots, translation systems, and content generation.
UPDATE: The model has now been released as open source for research purposes.""",
    
    "article_003": """Scientists have achieved a new milestone in quantum computing research.
The quantum processor demonstrated quantum advantage over classical computers.
This breakthrough brings us closer to practical quantum applications.
Potential uses include cryptography, drug discovery, and optimization problems.
CORRECTION: The processor uses 100 qubits, not 50 as previously reported."""
}

def generate_initial_data(output_dir: str = "data/test_news"):
    """Generate initial test data"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for article in ARTICLES:
        file_path = output_path / f"{article['id']}.txt"
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"{article['title']}\n\n{article['content']}")
    
    print(f"Generated {len(ARTICLES)} initial articles in {output_dir}")

def generate_modified_data(output_dir: str = "data/test_news_v2"):
    """Generate modified versions for CDC testing"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Copy all articles
    for article in ARTICLES:
        file_path = output_path / f"{article['id']}.txt"
        
        # Use modified version if available
        if article['id'] in MODIFIED_ARTICLES:
            content = MODIFIED_ARTICLES[article['id']]
        else:
            content = article['content']
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"{article['title']}\n\n{content}")
    
    print(f"Generated {len(ARTICLES)} articles (2 modified) in {output_dir}")

if __name__ == '__main__':
    print("Generating test data for CDC testing...\n")
    generate_initial_data()
    generate_modified_data()
    print("\nTest data ready!")
    print("\nUsage:")
    print("  1. Ingest initial data: python src/cli.py ingest data/test_news --reset")
    print("  2. Ingest modified data: python src/cli.py ingest data/test_news_v2")
    print("  3. Test Delta Lake: python tests/test_delta_lake.py")
