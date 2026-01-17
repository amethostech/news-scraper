"""
Hugging Face Transformer-based NER for Company Name Extraction

Uses pre-trained transformer models for higher accuracy entity extraction.
Recommended models:
- dslim/bert-base-NER (general purpose, fast)
- Jean-Baptiste/roberta-large-ner-english (higher accuracy)
- dbmdz/bert-large-cased-finetuned-conll03-english (good for ORG)

Usage:
    from huggingface_ner import HuggingFaceNER
    
    ner = HuggingFaceNER()
    entities = ner.extract_entities("Apple Inc announced a partnership with Microsoft.")
"""

import os
from typing import List, Dict, Optional
import warnings
warnings.filterwarnings("ignore")

# Set tokenizers parallelism before importing transformers
os.environ["TOKENIZERS_PARALLELISM"] = "false"


class HuggingFaceNER:
    """Hugging Face transformer-based NER extractor"""
    
    # Available models with their characteristics
    MODELS = {
        "fast": "dslim/bert-base-NER",           # Fast, good accuracy
        "accurate": "Jean-Baptiste/roberta-large-ner-english",  # Higher accuracy
        "balanced": "dbmdz/bert-large-cased-finetuned-conll03-english",  # Good balance
    }
    
    def __init__(self, model_type: str = "fast", device: str = None):
        """
        Initialize the Hugging Face NER model.
        
        Args:
            model_type: One of 'fast', 'accurate', or 'balanced'
            device: Device to use ('cuda', 'mps', 'cpu'). Auto-detected if None.
        """
        try:
            from transformers import pipeline, AutoTokenizer, AutoModelForTokenClassification
            import torch
        except ImportError:
            raise ImportError(
                "Please install transformers: pip install transformers torch"
            )
        
        # Select model
        if model_type in self.MODELS:
            model_name = self.MODELS[model_type]
        else:
            model_name = model_type  # Assume it's a direct model name
        
        # Auto-detect device
        if device is None:
            if torch.cuda.is_available():
                device = "cuda"
            elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
                device = "mps"
            else:
                device = "cpu"
        
        print(f"Loading Hugging Face NER model: {model_name}")
        print(f"Using device: {device}")
        
        # Load model and tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForTokenClassification.from_pretrained(model_name)
        
        # Create pipeline
        self.ner_pipeline = pipeline(
            "ner",
            model=self.model,
            tokenizer=self.tokenizer,
            device=0 if device == "cuda" else -1 if device == "cpu" else device,
            aggregation_strategy="simple"  # Merge B-ORG, I-ORG into single entity
        )
        
        self.model_name = model_name
        self.device = device
    
    def extract_entities(
        self, 
        text: str, 
        entity_types: List[str] = None,
        min_score: float = 0.7
    ) -> Dict[str, List[Dict]]:
        """
        Extract named entities from text.
        
        Args:
            text: Input text
            entity_types: List of entity types to extract (e.g., ['ORG', 'PER'])
                         If None, extracts all types
            min_score: Minimum confidence score (0-1) to include entity
        
        Returns:
            Dictionary with entity types as keys and lists of entity dicts as values
        """
        if not text or not text.strip():
            return {}
        
        # Truncate very long texts (transformer limit)
        max_length = 512 * 4  # Approximate character limit
        if len(text) > max_length:
            text = text[:max_length]
        
        try:
            results = self.ner_pipeline(text)
        except Exception as e:
            print(f"NER error: {e}")
            return {}
        
        # Group by entity type
        entities = {}
        
        for ent in results:
            # Get entity type (remove B-, I- prefixes if present)
            ent_type = ent.get("entity_group", ent.get("entity", "UNKNOWN"))
            ent_type = ent_type.replace("B-", "").replace("I-", "")
            
            # Filter by score
            score = ent.get("score", 0)
            if score < min_score:
                continue
            
            # Filter by type if specified
            if entity_types and ent_type not in entity_types:
                continue
            
            if ent_type not in entities:
                entities[ent_type] = []
            
            entities[ent_type].append({
                "text": ent["word"].strip(),
                "score": round(score, 4),
                "start": ent.get("start"),
                "end": ent.get("end")
            })
        
        # Remove duplicates (keep highest score)
        for ent_type in entities:
            seen = {}
            for ent in entities[ent_type]:
                text_lower = ent["text"].lower()
                if text_lower not in seen or seen[text_lower]["score"] < ent["score"]:
                    seen[text_lower] = ent
            entities[ent_type] = list(seen.values())
        
        return entities
    
    def extract_companies(self, text: str, min_score: float = 0.7) -> List[str]:
        """
        Extract only company/organization names from text.
        
        Args:
            text: Input text
            min_score: Minimum confidence score
        
        Returns:
            List of company names
        """
        entities = self.extract_entities(text, entity_types=["ORG"], min_score=min_score)
        
        companies = []
        for ent in entities.get("ORG", []):
            name = ent["text"].strip()
            # Clean up tokenizer artifacts
            name = name.replace("##", "").strip()
            if name and len(name) > 1:
                companies.append(name)
        
        return companies
    
    def batch_extract_companies(
        self, 
        texts: List[str], 
        min_score: float = 0.7,
        show_progress: bool = True
    ) -> List[List[str]]:
        """
        Extract companies from multiple texts.
        
        Args:
            texts: List of input texts
            min_score: Minimum confidence score
            show_progress: Whether to show progress bar
        
        Returns:
            List of company lists (one per input text)
        """
        if show_progress:
            try:
                from tqdm import tqdm
                texts = tqdm(texts, desc="Extracting entities")
            except ImportError:
                pass
        
        results = []
        for text in texts:
            companies = self.extract_companies(text, min_score=min_score)
            results.append(companies)
        
        return results


def test_huggingface_ner():
    """Quick test of the Hugging Face NER"""
    print("=" * 60)
    print("Testing Hugging Face NER")
    print("=" * 60)
    
    # Test texts
    test_texts = [
        "Apple Inc. announced a partnership with Microsoft Corporation to develop AI tools.",
        "Pfizer and BioNTech reported positive results from their latest clinical trial.",
        "Tesla CEO Elon Musk met with executives from Goldman Sachs in New York.",
        "Johnson & Johnson acquired Abiomed for $16.6 billion.",
        "The FDA approved Merck's new cancer drug Keytruda for additional indications.",
    ]
    
    # Initialize NER
    ner = HuggingFaceNER(model_type="fast")
    
    print("\nTest Results:")
    print("-" * 60)
    
    for text in test_texts:
        companies = ner.extract_companies(text)
        print(f"\nText: {text[:80]}...")
        print(f"Companies: {', '.join(companies) if companies else 'None found'}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    test_huggingface_ner()
