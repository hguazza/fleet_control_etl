from typing import Dict
import pandas as pd



def transform(df: pd.DataFrame) -> pd.DataFrame:

    placa_motorista: Dict[str : str] = {
    "FED9247": "Luiz",
    "GBX2G51": "Mylena",
    "SVD6D88": "Carlos",
    "EPI6184": "Caminhão Baú",
    "BWK2969": "Caminhaão Granel"
}

    df_transformed = df.copy()
    df_transformed['Motorista'] = df_transformed['Placa'].map(placa_motorista).fillna('')
    
    return df_transformed
