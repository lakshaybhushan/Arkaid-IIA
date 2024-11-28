import yaml
import json
from typing import Dict, List, Set
import psycopg2
from psycopg2.extras import RealDictCursor
from jellyfish import jaro_winkler_similarity
import re

class SchemaMapper:
    def __init__(self, db_config_path: str, mapping_file_path: str):
        # Load configurations
        self.db_config = self._load_yaml(db_config_path)
        self.mapping_config = self._load_json(mapping_file_path)
        
        # Store target schema
        self.target_schema = self.mapping_config["target_schema"]
        
        # Common gaming-related terms to remove
        self.common_terms = [
            'game', 'games', 'gaming',
            'steam', 'epic',
            'app', 'application',
            'product'
        ]
        
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
        """Fetch schemas from source tables"""
        source_schemas = {}
        
        for source in self.mapping_config["sources"]:
            conn = self.get_db_connection(source["database"])
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get column information from the source table
            cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{source["name"]}'
            """)
            
            columns = cur.fetchall()
            source_schemas[source["name"]] = {
                col["column_name"]: col["data_type"] 
                for col in columns
            }
            
            cur.close()
            conn.close()
            
        return source_schemas

    def preprocess_column_name(self, column_name: str) -> str:
        """Preprocess column name by removing common terms and normalizing"""
        # Convert to lowercase and replace underscores/hyphens with spaces
        processed = column_name.lower()
        processed = re.sub(r'[_-]', ' ', processed)
        
        # Remove common gaming-related terms
        for term in self.common_terms:
            # Remove term with various separators (_, -, space)
            processed = re.sub(f'{term}_', '', processed)
            processed = re.sub(f'{term}-', '', processed)
            processed = re.sub(f'{term} ', '', processed)
            processed = re.sub(f'_{term}', '', processed)
            processed = re.sub(f'-{term}', '', processed)
            processed = re.sub(f' {term}', '', processed)
            # Remove term if it's the entire string
            if processed == term:
                processed = processed.replace(term, '')
        
        # Remove any double spaces and trim
        processed = ' '.join(processed.split())
        
        # Remove any remaining leading/trailing separators
        processed = processed.strip('_- ')
        
        return processed

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate Jaro-Winkler similarity between two preprocessed column names"""
        # Preprocess both texts
        processed_text1 = self.preprocess_column_name(text1)
        processed_text2 = self.preprocess_column_name(text2)
        
        # If either string is empty after preprocessing, use original strings
        if not processed_text1 or not processed_text2:
            return jaro_winkler_similarity(text1, text2)
        
        # Calculate similarity on processed strings
        similarity = jaro_winkler_similarity(processed_text1, processed_text2)
        
        # For debugging
        if similarity > 0.85:
            print(f"Match found: '{text1}' ({processed_text1}) -> '{text2}' ({processed_text2}) = {similarity}")
            
        return similarity

    def generate_schema_mappings(self) -> Dict[str, Dict[str, str]]:
        """Generate mappings between source and target schemas"""
        source_schemas = self.get_source_schemas()
        schema_mappings = {}
        matched_target_columns = set()
        
        print("\nStarting schema mapping process...")
        
        # For each source table
        for source_name, source_schema in source_schemas.items():
            print(f"\nProcessing table: {source_name}")
            column_mappings = {}
            table_matched_columns = set()
            
            # For each column in source
            for source_col in source_schema.keys():
                best_match = None
                best_score = 0
                
                # Find best matching target column
                for target_col in self.target_schema.keys():
                    if target_col == 'id':  # Skip ID column as it's auto-generated
                        continue
                        
                    similarity = self.calculate_similarity(source_col, target_col)
                    
                    if similarity > best_score and similarity > 0.85:
                        best_score = similarity
                        best_match = target_col
                
                if best_match:
                    column_mappings[source_col] = best_match
                    table_matched_columns.add(best_match)
                else:
                    print(f"Warning: No match found for source column '{source_col}' "
                          f"(processed: '{self.preprocess_column_name(source_col)}')")
            
            schema_mappings[source_name] = column_mappings
            matched_target_columns.update(table_matched_columns)
            
            # Print unmatched columns for this table
            print(f"\nUnmatched target columns for table {source_name}:")
            unmatched = set(self.target_schema.keys()) - table_matched_columns - {'id'}
            if unmatched:
                for col in unmatched:
                    print(f"- {col} (processed: '{self.preprocess_column_name(col)}')")
            else:
                print("All target columns are matched!")
        
        self._save_mappings(schema_mappings)
        return schema_mappings
    
    def _save_mappings(self, mappings: Dict[str, Dict[str, str]]):
        """Save generated mappings to a JSON file"""
        output_path = f"mappings/{self.mapping_config['materialized_view']}_schema_mappings.json"
        with open(output_path, 'w') as f:
            json.dump(mappings, f, indent=2)

if __name__ == "__main__":
    mapper = SchemaMapper(
        db_config_path='db_config.yaml',
        mapping_file_path='mappings/mv_games.json'
    )
    
    # Generate and print mappings
    mappings = mapper.generate_schema_mappings()
    print("\nGenerated schema mappings:")
    print(json.dumps(mappings, indent=2)) 