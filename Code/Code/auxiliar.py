import pandas as pd

arq = pd.read_csv("C:\dados.csv", delimiter=";", low_memory=False)

print(arq)

arq = arq.assign(
    DATA_PESQUISA=lambda x: pd.to_datetime(x.DATA_PESQUISA)
).query("DATA_PESQUISA >= '2019-07-01' and DATA_PESQUISA <= '2019-07-31'")

print(arq)

arq.to_csv("../Data/Disque_Economia.csv", sep=";", index=False)