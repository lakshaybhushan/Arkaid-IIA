import yaml
import json
from typing import Dict, List, Set, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
from jellyfish import jaro_winkler_similarity
import re
import spacy
from collections import defaultdict

class SchemaMapper:
    def __init__(self, db_config_path: str, mapping_file_path: str):
        # Load configurations
        self.db_config = self._load_yaml(db_config_path)
        self.mapping_config = self._load_json(mapping_file_path)
        
        # Store target schema
        self.target_schema = self.mapping_config["target_schema"]
        
        # Load spaCy model
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            print("Downloading spaCy model...")
            spacy.cli.download("en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
        
        # Define domain-specific entities
        self.domain_entities = {
            'GAME': ['game', 'gaming', 'steam', 'epic', 'platform'],
            'CREATOR': ['creator', 'streamer', 'content', 'channel'],
            'METRIC': ['views', 'revenue', 'price', 'count', 'number'],
            'IDENTIFIER': ['id', 'identifier', 'key'],
            'METADATA': ['name', 'title', 'type', 'category', 'genre']
        }
        
        # Create entity patterns for spaCy
        self.entity_patterns = self._create_entity_patterns()
        
        # Add entity ruler to pipeline
        if 'entity_ruler' not in self.nlp.pipe_names:
            ruler = self.nlp.add_pipe('entity_ruler', before='ner')
            ruler.add_patterns(self.entity_patterns)

    def _create_entity_patterns(self) -> List[Dict]:
        """Create patterns for spaCy's entity ruler"""
        patterns = []
        for entity_type, terms in self.domain_entities.items():
            for term in terms:
                patterns.append({
                    "label": entity_type,
                    "pattern": term
                })
                # Add common variations
                patterns.append({
                    "label": entity_type,
                    "pattern": f"{term}s"
                })
        return patterns

    def extract_entities(self, text: str) -> List[Tuple[str, str]]:
        """Extract entities from column name using spaCy"""
        # Preprocess text
        text = re.sub(r'[_-]', ' ', text.lower())
        
        # Process text with spaCy
        doc = self.nlp(text)
        
        # Extract entities and their labels
        entities = [(ent.text, ent.label_) for ent in doc.ents]
        
        # If no entities found, try matching with domain entities directly
        if not entities:
            for entity_type, terms in self.domain_entities.items():
                for term in terms:
                    if term in text:
                        entities.append((term, entity_type))
        
        return entities

    def calculate_semantic_similarity(self, source_col: str, target_col: str) -> float:
        """Calculate similarity based on semantic meaning and entities"""
        # Extract entities from both columns
        source_entities = self.extract_entities(source_col)
        target_entities = self.extract_entities(target_col)
        
        # If no entities found, fall back to string similarity
        if not source_entities or not target_entities:
            return jaro_winkler_similarity(source_col, target_col)
        
        # Calculate entity overlap score
        shared_entities = set(source_entities) & set(target_entities)
        entity_score = len(shared_entities) / max(len(source_entities), len(target_entities))
        
        # Check if entities are of the same type
        source_types = {e[1] for e in source_entities}
        target_types = {e[1] for e in target_entities}
        type_match = len(source_types & target_types) > 0
        
        # Calculate string similarity as fallback
        string_sim = jaro_winkler_similarity(source_col, target_col)
        
        # Combine scores with weights
        final_score = (0.6 * entity_score) + (0.4 * string_sim)
        
        # Boost score if entity types match
        if type_match:
            final_score = min(1.0, final_score * 1.2)
            
        return final_score

    def generate_schema_mappings(self) -> Dict[str, Dict[str, str]]:
        """Generate mappings between source and target schemas using NER"""
        source_schemas = self.get_source_schemas()
        schema_mappings = {}
        
        print("\nStarting schema mapping process with NER...")
        
        for source_name, source_schema in source_schemas.items():
            print(f"\nProcessing table: {source_name}")
            column_mappings = {}
            
            for source_col in source_schema.keys():
                best_match = None
                best_score = 0
                
                # Extract source column entities
                source_entities = self.extract_entities(source_col)
                print(f"\nAnalyzing {source_col}:")
                print(f"Entities found: {source_entities}")
                
                # Find best matching target column
                for target_col in self.target_schema.keys():
                    if target_col == 'id':  # Skip ID column as it's auto-generated
                        continue
                    
                    similarity = self.calculate_semantic_similarity(source_col, target_col)
                    
                    if similarity > best_score and similarity > 0.85:
                        best_score = similarity
                        best_match = target_col
                        print(f"Potential match: {target_col} (score: {similarity:.2f})")
                
                if best_match:
                    column_mappings[source_col] = best_match
                    print(f"Selected match: {source_col} -> {best_match} (score: {best_score:.2f})")
                else:
                    print(f"Warning: No match found for source column '{source_col}'")
            
            schema_mappings[source_name] = column_mappings
        
        self._save_mappings(schema_mappings)
        return schema_mappings

    def _load_yaml(self, path: str) -> dict:
        with open(path, 'r') as file:
            return yaml.safe_load(file)
            
    def _load_json(self, path: str) -> dict:
        with open(path, 'r') as file:
            return json.load(file)
            
    def get_db_connection(self, db_name: str):
        """Create database connection based on database name"""
        db_info = next(db for db in self.db_config["databases"] 
                      if db["name"] == db_name)
        
        return psycopg2.connect(
            host=db_info["host"],
            port=db_info["port"],
            dbname=db_info["dbname"],
            user=db_info["username"],
            password=db_info["password"]
        )

    def get_source_schemas(self) -> Dict[str, List[str]]:
        """Fetch schemas from source tables/materialized views"""
        source_schemas = {}
        
        for source in self.mapping_config["sources"]:
            conn = self.get_db_connection(source["database"])
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Different query based on database (DB3 uses materialized views)
            if source["database"] == "DB3":
                # First check if it's a materialized view
                cur.execute(f"""
                    SELECT schemaname, matviewname, definition 
                    FROM pg_matviews 
                    WHERE schemaname = 'public' 
                    AND matviewname = '{source["name"]}'
                """)
                
                if cur.fetchone():
                    # Get column information from materialized view
                    cur.execute(f"""
                        SELECT a.attname as column_name, 
                               pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
                        FROM pg_catalog.pg_attribute a
                        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = '{source["name"]}'
                        AND n.nspname = 'public'
                        AND a.attnum > 0 
                        AND NOT a.attisdropped
                        ORDER BY a.attnum
                    """)
                else:
                    print(f"Warning: {source['name']} is not a materialized view in DB3")
                    continue
            else:
                # Get column information from regular table
                cur.execute(f"""
                    SELECT c.column_name, c.data_type 
                    FROM information_schema.columns c
                    JOIN information_schema.tables t 
                        ON c.table_name = t.table_name 
                        AND c.table_schema = t.table_schema
                    WHERE c.table_name = '{source["name"]}'
                    AND c.table_schema = 'public'
                    AND t.table_type = 'BASE TABLE'
                """)
            
            columns = cur.fetchall()
            source_schemas[source["name"]] = {
                col["column_name"]: col["data_type"] 
                for col in columns
            }
            
            cur.close()
            conn.close()
            
        return source_schemas

    def _save_mappings(self, mappings: Dict[str, Dict[str, str]]):
        """Save generated mappings to a JSON file"""
        output_path = f"mappings/{self.mapping_config['materialized_view']}_schema_mappings.json"
        with open(output_path, 'w') as f:
            json.dump(mappings, f, indent=2)

if __name__ == "__main__":
    mapper = SchemaMapper(
        db_config_path='db_config.yaml',
        mapping_file_path='mappings/mv_content_creator_mv_games.json'
    )
    
    # Generate and print mappings
    mappings = mapper.generate_schema_mappings()
    print("\nGenerated schema mappings:")
    print(json.dumps(mappings, indent=2)) 