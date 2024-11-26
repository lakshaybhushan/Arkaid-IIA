from qa import QueryInsights
from qd import QueryDecomposer

def analyze_and_decompose_query(query):
    print("=" * 50)
    print("Original Query:")
    print(query)
    print("=" * 50)
    
    # Get query insights
    print("\n1. Query Analysis:")
    print("-" * 30)
    analyzer = QueryInsights(query)
    analysis = analyzer.get_detailed_analysis()
    
    for key, value in analysis.items():
        print(f"\n{key.replace('_', ' ').title()}:")
        print(value)
    
    # Decompose query
    print("\n" + "=" * 50)
    print("2. Query Decomposition:")
    print("-" * 30)
    
    decomposer = QueryDecomposer(query)
    subqueries = decomposer.generate_table_subqueries()
    join_conditions = decomposer.get_join_conditions()
    
    print("\nGenerated Subqueries:")
    for table, subquery in subqueries.items():
        print(f"\n{table}:")
        print(subquery)
    
    print("\nJoin Conditions:")
    for tables, join_info in join_conditions.items():
        print(f"\nBetween {' and '.join(tables)}:")
        print(f"Type: {join_info['type']}")
        print(f"Condition: {join_info['condition']}")

if __name__ == "__main__":
    # Example 1: Simple query with join and aggregation
    query1 = """
    SELECT p.Country, AVG(ps.hrs_played) AS Avg_Hours_Played
    FROM players p
    JOIN player_stats ps ON p.ID = ps.playerId
    GROUP BY p.Country;
    """
    
    # Example 2: More complex query with multiple joins
    query2 = """
    SELECT 
        p.Country, 
        d.DepartmentName,
        AVG(ps.hrs_played) AS Avg_Hours, 
        MAX(ps.achievements_completed) AS Max_Achievements,
        COUNT(DISTINCT ps.playerId) as Player_Count
    FROM players p
    LEFT JOIN player_stats ps ON p.ID = ps.playerId
    INNER JOIN departments d ON p.dept_id = d.id
    GROUP BY p.Country, d.DepartmentName
    HAVING AVG(ps.hrs_played) > 10
    ORDER BY Avg_Hours DESC, Player_Count ASC;
    """
    
    print("\nAnalyzing Query 1:")
    analyze_and_decompose_query(query1)
    
    # print("\n\nAnalyzing Query 2:")
    # analyze_and_decompose_query(query2)
