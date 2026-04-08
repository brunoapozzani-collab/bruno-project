# Extrator de Despesas Fixas — TAAG Brasil

Aplicativo web em Python/Streamlit que lê um razão geral em Excel, classifica
cada lançamento por palavra-chave em categorias e empresas definidas pelo
usuário, e gera um **resumo executivo em PDF** com gráficos e análises, além
de uma planilha Excel detalhada por empresa.

## Como usar

1. **Carregue o razão geral** (`.xlsx`) na seção 1.
2. **Confira o mapeamento das colunas** na seção 2 (data, descrição, valor,
   etc. são detectados automaticamente).
3. Na **barra lateral**, importe o seu arquivo `categorias.xlsx` (ou edite as
   regras direto na tabela). Cada regra define:
   - **palavra-chave** — texto a procurar na descrição/conta
   - **categoria** — nome da despesa (ex: Aluguel, Água)
   - **empresa** — empresa à qual a despesa será atribuída
4. Ajuste o **período** e clique em **🚀 Processar e gerar relatório**.
5. **Baixe o PDF executivo e o Excel detalhado**.

> ⚠️ **As regras NÃO ficam salvas no servidor.** Ao final da sessão, clique em
> **📥 Baixar regras** para guardar seu arquivo. Na próxima visita, use
> **📤 Importar regras** para carregá-lo de volta.

## Rodando localmente

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

---
[taagbrasil.com.br](https://taagbrasil.com.br) · Liderança em Automação,
Áudio e Vídeo desde 1997
