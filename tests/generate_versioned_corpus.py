"""Generate realistic versioned document corpus for benchmarking"""
import os
import random
from datetime import datetime, timedelta

def generate_versioned_corpus(num_docs=100, num_versions=5, output_dir="data/benchmark_corpus"):
    """Generate documents with realistic version evolution
    
    Args:
        num_docs: Number of unique documents
        num_versions: Versions per document
        output_dir: Output directory
    """
    os.makedirs(output_dir, exist_ok=True)
    
    topics = ["AI", "Machine Learning", "Cloud Computing", "Cybersecurity", "Data Science"]
    
    base_date = datetime(2024, 1, 1)
    
    for doc_id in range(1, num_docs + 1):
        topic = random.choice(topics)
        
        for version in range(1, num_versions + 1):
            # Simulate version date (1 month apart)
            version_date = base_date + timedelta(days=30 * (version - 1))
            
            content = f"""Document: {topic} Policy v{version}
Last Updated: {version_date.strftime('%Y-%m-%d')}

Section 1: Overview
{topic} is a critical technology for modern enterprises. This policy outlines guidelines for implementation and usage.

Section 2: Requirements (Version {version})
- Requirement A: {'Updated in v' + str(version) if version > 1 else 'Initial requirement'}
- Requirement B: Standard compliance mandatory
- Requirement C: {'Added in v' + str(version) if version >= 3 else 'Security protocols'}

Section 3: Implementation
Implementation must follow industry best practices. {'New guidance added in version ' + str(version) + '.' if version > 2 else 'Standard procedures apply.'}

Section 4: Compliance
All implementations must be audited quarterly. {'Audit frequency changed in v' + str(version) if version == 4 else 'Standard audit procedures.'}
"""
            
            # Write version file
            filename = f"{output_dir}/doc_{doc_id:03d}_v{version}.txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)
    
    print(f"Generated {num_docs} documents with {num_versions} versions each")
    print(f"Total files: {num_docs * num_versions}")
    print(f"Output: {output_dir}")

if __name__ == "__main__":
    generate_versioned_corpus(num_docs=100, num_versions=5)
