import os

def write_to_doris(df, epoch_id, table_name, columns_list):
    """
    Writes a PySpark DataFrame to Apache Doris using the official Doris Spark Connector.
    Since this uses the DataSource V2 Connector, it handles distributed writing,
    buffering, routing, and reliability natively under the hood.
    """
    # Select columns present in df to avoid schema mismatch errors
    select_cols = [c for c in columns_list if c in df.columns]
    df_selected = df.select(*select_cols)
    
    doris_user = os.getenv("DORIS_USER", "root")
    doris_password = os.getenv("DORIS_PASSWORD", "")
    
    doris_fe_nodes = os.getenv("DORIS_FE_NODES", "doris-fe:8030")
    
    df_selected.write \
        .format("doris") \
        .option("doris.fenodes", doris_fe_nodes) \
        .option("doris.table.identifier", f"thelook_dw.{table_name}") \
        .option("doris.user", doris_user) \
        .option("doris.password", doris_password) \
        .mode("append") \
        .save()
