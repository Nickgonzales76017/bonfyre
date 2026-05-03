#!/usr/bin/env python3
"""
Create and populate Aurekai repos on HuggingFace
"""

from huggingface_hub import (
    create_repo,
    upload_file,
    hf_api,
)
import os

# Repository configurations
repos = [
    {
        "name": "model-memory",
        "type": "model",
        "description": "Public model memory archive for Aurekai platform. Stores compiled model representations, SAE dictionaries, and semantic embeddings.",
    },
    {
        "name": "sae-dictionaries",
        "type": "model",
        "description": "Sparse autoencoder (SAE) dictionary repository for Aurekai. Provides model interpretability through SAE coefficients.",
    },
    {
        "name": "fpqx-alignments",
        "type": "model",
        "description": "Feature-to-proxy quantization (FPQx) alignment repository for Aurekai. Enables cross-model semantic routing.",
    },
    {
        "name": "semantic-cache-bench",
        "type": "model",
        "description": "Semantic caching benchmarks and performance suite for Aurekai. Validates cache consistency and hit rates.",
    },
]

readme_map = {
    "model-memory": "/Users/nickgonzales/Documents/Bonfyre/hf-repos/model-memory-README.md",
    "sae-dictionaries": "/Users/nickgonzales/Documents/Bonfyre/hf-repos/sae-dictionaries-README.md",
    "fpqx-alignments": "/Users/nickgonzales/Documents/Bonfyre/hf-repos/fpqx-alignments-README.md",
    "semantic-cache-bench": "/Users/nickgonzales/Documents/Bonfyre/hf-repos/semantic-cache-bench-README.md",
}

files_to_upload = {
    "model-memory": [
        ("/Users/nickgonzales/Documents/Bonfyre/Aurekai/dist/aurekai-model-memory-qwen3-8b-20260502.tar.gz", "aurekai-model-memory-qwen3-8b-20260502.tar.gz"),
        ("/Users/nickgonzales/Documents/Bonfyre/Aurekai/dist/aurekai.manifest.json", "aurekai.manifest.json"),
        ("/Users/nickgonzales/Documents/Bonfyre/Aurekai/dist/bonfyre.manifest.json", "bonfyre.manifest.json"),
    ],
    "sae-dictionaries": [],
    "fpqx-alignments": [],
    "semantic-cache-bench": [],
}

def create_and_populate_repos():
    """Create repos and upload READMEs"""
    
    org_name = "aurekai"
    
    for repo_config in repos:
        repo_name = repo_config["name"]
        repo_type = repo_config["type"]
        description = repo_config["description"]
        
        print(f"\n{'='*60}")
        print(f"Creating: {org_name}/{repo_name}")
        print(f"Type: {repo_type}")
        print(f"Description: {description}")
        print(f"{'='*60}")
        
        try:
            # Create repo
            repo_url = create_repo(
                repo_id=f"{org_name}/{repo_name}",
                repo_type=repo_type,
                private=False,
                exist_ok=True,  # Don't fail if already exists
            )
            print(f"✓ Repo created/exists: {repo_url}")
            
        except Exception as e:
            print(f"✗ Error creating repo: {e}")
            continue
        
        # Upload README
        readme_path = readme_map.get(repo_name)
        if readme_path and os.path.exists(readme_path):
            try:
                print(f"Uploading README from {readme_path}...")
                upload_file(
                    path_or_fileobj=readme_path,
                    path_in_repo="README.md",
                    repo_id=f"{org_name}/{repo_name}",
                    repo_type=repo_type,
                    commit_message=f"Initial README for {repo_name}",
                )
                print(f"✓ README uploaded")
            except Exception as e:
                print(f"✗ Error uploading README: {e}")
        else:
            print(f"⚠ README not found: {readme_path}")
        
        # Upload other files
        files = files_to_upload.get(repo_name, [])
        for local_path, remote_name in files:
            if os.path.exists(local_path):
                try:
                    print(f"Uploading {remote_name}...")
                    upload_file(
                        path_or_fileobj=local_path,
                        path_in_repo=remote_name,
                        repo_id=f"{org_name}/{repo_name}",
                        repo_type=repo_type,
                        commit_message=f"Add {remote_name}",
                    )
                    print(f"✓ {remote_name} uploaded")
                except Exception as e:
                    print(f"✗ Error uploading {remote_name}: {e}")
            else:
                print(f"⚠ File not found: {local_path}")
    
    print(f"\n{'='*60}")
    print("All repos created and populated!")
    print(f"{'='*60}")
    print("\nNext steps:")
    print("1. Visit https://huggingface.co/aurekai to view your org")
    print("2. Verify all repos are accessible:")
    for repo in repos:
        print(f"   - https://huggingface.co/aurekai/{repo['name']}")

if __name__ == "__main__":
    create_and_populate_repos()
