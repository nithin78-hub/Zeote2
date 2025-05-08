import csv
import time
import pandas as pd
from clickhouse_driver import Client

class ClickHouseManager:
    def __init__(self, host, port, database, user, jwt_token=None):
        """
        Initialize ClickHouse client with connection parameters.
        """
        # Create connection settings based on auth method
        self.host = host
        self.port = port
        self.database = database
        
        # Configure client with JWT token authentication if provided
        if jwt_token:
            # Using JWT token auth
            self.client = Client(
                host=host,
                port=port,
                database=database,
                user=user,
                password=None,  # No password when using JWT
                settings={
                    'jwt_auth_header': {
                        'Authorization': f'Bearer {jwt_token}'
                    }
                }
            )
        else:
            # Using regular user authentication (for target)
            self.client = Client(
                host=host,
                port=port,
                database=database,
                user=user
            )
    
    def get_tables(self):
        """
        Get list of tables in the database.
        """
        query = f"SHOW TABLES FROM {self.database}"
        result = self.client.execute(query)
        return [table[0] for table in result]
    
    def get_columns(self, table):
        """
        Get column definitions for a table.
        """
        query = f"DESCRIBE TABLE {table}"
        result = self.client.execute(query)
        columns = []
        
        for col in result:
            columns.append({
                'name': col[0],
                'type': col[1],
                'default_type': col[2],
                'default_expression': col[3]
            })
        
        return columns
    
    def preview_data(self, table, columns=None, limit=100):
        """
        Get preview data for a table with selected columns.
        """
        cols = '*'
        if columns and len(columns) > 0:
            cols = ', '.join(f'`{col}`' for col in columns)
        
        query = f"SELECT {cols} FROM {table} LIMIT {limit}"
        result = self.client.execute(query, with_column_types=True)
        
        # Process result with column names
        column_names = [col[0] for col in result[1]]
        data = []
        
        for row in result[0]:
            data_row = {}
            for i, value in enumerate(row):
                data_row[column_names[i]] = value
            data.append(data_row)
        
        return data
    
    def preview_join_data(self, join_config, columns=None, limit=100):
        """
        Get preview data for a join query with selected columns.
        """
        # Construct JOIN query from config
        base_table = join_config.get('base_table')
        join_tables = join_config.get('join_tables', [])
        join_conditions = join_config.get('join_conditions', [])
        
        # Validate join configuration
        if not base_table or not join_tables or len(join_tables) != len(join_conditions):
            raise ValueError("Invalid join configuration")
        
        # Select columns
        cols = '*'
        if columns and len(columns) > 0:
            cols = ', '.join(f'`{col}`' for col in columns)
        
        # Build query
        query = f"SELECT {cols} FROM {base_table}"
        
        for i, join_table in enumerate(join_tables):
            join_type = join_config.get('join_types', ['JOIN'])[i] if 'join_types' in join_config else 'JOIN'
            query += f" {join_type} {join_table} ON {join_conditions[i]}"
        
        query += f" LIMIT {limit}"
        
        # Execute query
        result = self.client.execute(query, with_column_types=True)
        
        # Process result with column names
        column_names = [col[0] for col in result[1]]
        data = []
        
        for row in result[0]:
            data_row = {}
            for i, value in enumerate(row):
                data_row[column_names[i]] = value
            data.append(data_row)
        
        return data

    def export_to_file(self, table, columns, output_path, delimiter=',', batch_size=10000):
        """
        Export data from ClickHouse table to a flat file.
        """
        cols = '*'
        if columns and len(columns) > 0:
            cols = ', '.join(f'`{col}`' for col in columns)
        
        # Query for count
        count_query = f"SELECT COUNT(*) FROM {table}"
        total_count = self.client.execute(count_query)[0][0]
        
        # Main data query
        query = f"SELECT {cols} FROM {table}"
        
        # Use settings to stream in batches
        settings = {'max_block_size': batch_size}
        
        # Get column info for header
        result = self.client.execute(query + " LIMIT 0", with_column_types=True, settings=settings)
        header = [col[0] for col in result[1]]
        
        # Write to CSV in batches
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerow(header)
            
            # Stream data in batches to avoid memory issues
            rows_processed = 0
            for batch in self.client.execute_iter(query, settings=settings):
                writer.writerow(batch)
                rows_processed += 1
        
        return rows_processed
    
    def export_join_to_file(self, join_config, columns, output_path, delimiter=',', batch_size=10000):
        """
        Export data from a JOIN query to a flat file.
        """
        # Construct JOIN query from config
        base_table = join_config.get('base_table')
        join_tables = join_config.get('join_tables', [])
        join_conditions = join_config.get('join_conditions', [])
        
        # Validate join configuration
        if not base_table or not join_tables or len(join_tables) != len(join_conditions):
            raise ValueError("Invalid join configuration")
        
        # Select columns
        cols = '*'
        if columns and len(columns) > 0:
            cols = ', '.join(f'`{col}`' for col in columns)
        
        # Build query
        query = f"SELECT {cols} FROM {base_table}"
        
        for i, join_table in enumerate(join_tables):
            join_type = join_config.get('join_types', ['JOIN'])[i] if 'join_types' in join_config else 'JOIN'
            query += f" {join_type} {join_table} ON {join_conditions[i]}"
        
        # Use settings to stream in batches
        settings = {'max_block_size': batch_size}
        
        # Get column info for header
        result = self.client.execute(query + " LIMIT 0", with_column_types=True, settings=settings)
        header = [col[0] for col in result[1]]
        
        # Write to CSV in batches
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerow(header)
            
            # Stream data in batches to avoid memory issues
            rows_processed = 0
            for batch in self.client.execute_iter(query, settings=settings):
                writer.writerow(batch)
                rows_processed += 1
        
        return rows_processed
    
    def import_from_file(self, flat_file_manager, columns, target_table, create_table=False):
        """
        Import data from a flat file to ClickHouse.
        """
        # Get data as DataFrame
        df = flat_file_manager.get_data(columns)
        
        # Create table if needed
        if create_table:
            self._create_table_from_dataframe(df, target_table)
        
        # Insert data in batches
        batch_size = 10000
        total_rows = len(df)
        total_inserted = 0
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:min(i+batch_size, total_rows)]
            batch_data = batch_df.to_dict('records')
            
            # Convert dict to tuple list for clickhouse-driver
            column_names = batch_df.columns.tolist()
            values = [[row[col] for col in column_names] for row in batch_data]
            
            if values:  # Make sure we have data to insert
                self.client.execute(
                    f"INSERT INTO {target_table} ({', '.join(f'`{col}`' for col in column_names)}) VALUES",
                    values
                )
                total_inserted += len(values)
        
        return total_inserted
    
    def _create_table_from_dataframe(self, df, table_name):
        """
        Create a table based on DataFrame schema.
        """
        # Map pandas dtypes to ClickHouse types
        type_mapping = {
            'int64': 'Int64',
            'int32': 'Int32',
            'float64': 'Float64',
            'float32': 'Float32',
            'bool': 'UInt8',
            'datetime64[ns]': 'DateTime',
            'object': 'String',  # Default for string and other objects
        }
        
        # Create column definitions
        columns = []
        for col_name, dtype in df.dtypes.items():
            ch_type = type_mapping.get(str(dtype), 'String')
            columns.append(f"`{col_name}` {ch_type}")
        
        # Create table query
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)}) ENGINE = MergeTree() ORDER BY tuple()"
        
        # Execute query
        self.client.execute(create_query)