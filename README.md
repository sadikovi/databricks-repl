# databricks-repl
Unofficial Databricks REPL

Example of authenticating in [Databricks Community Edition](https://databricks.com/product/faq/community-edition)
or [Databricks Platform](https://databricks.com/product/unified-analytics-platform) with user-based authentication.

Does not use [Databricks Token API](https://docs.databricks.com/api/latest/tokens.html), so it can be used in CE as well.

## Usage
```python
db = DatabricksApi("https://community.cloud.databricks.com")

session = db.login("user", "password")

# by default, user has only one workspace
workspaces = session.list_workspaces()

workspace = workspaces[0]

clusters = workspace.list_clusters()
```
