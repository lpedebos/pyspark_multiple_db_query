import json
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def create_spark_session():
    return SparkSession.builder \
        .appName("MultiPostgresConnection") \
        .config("spark.jars", "postgresql-42.7.4.jar") \
        .getOrCreate()

def read_query_from_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        query = file.read().strip()  # Remove blank spaces
        query = query.rstrip(';') # Remove ";" at the end of file, if exists
    return query

def read_from_postgres(spark: SparkSession, url: str, query: str, user: str, password: str) -> DataFrame:
    return spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("query", query) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def load_config(file_path: str):
    with open(file_path, 'r') as file:
        return json.load(file)

def execute_query_on_combined_df(spark: SparkSession, combined_df: DataFrame, query: str) -> DataFrame:
    combined_df.createOrReplaceTempView("combined_table") # set name for combined dataframe
    return spark.sql(query)

def save_df_to_csv(df: DataFrame, file_path: str, sep: str = ";"):
    with open(file_path, 'w') as file:
        header = sep.join(df.columns)
        file.write(header + "\n")
        for row in df.collect():
            line = sep.join([str(item) for item in row])
            file.write(line + "\n")

# Usage example
if __name__ == "__main__":
    spark = create_spark_session()
    config = load_config('config.json')
    query = read_query_from_file('first_query.sql')
    second_query = read_query_from_file('second_query.sql')
    
    dataframes = []
    
    for db in config['databases']:
        df = read_from_postgres(
            spark, 
            url=db['url'],
            query=query,
            user=db['user'],
            password=db['password']
        )
        dataframes.append(df)
    
    # Union of dataframes
    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.union(df)
    
    # Execution of the second query on combined dataframe
    result_df = execute_query_on_combined_df(spark, combined_df, second_query)
    
    # Save results to a CSV file
    save_df_to_csv(result_df, "results.csv")

    print("Resultados salvos em results.csv")
