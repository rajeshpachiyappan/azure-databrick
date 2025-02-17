# Databricks notebook source
# MAGIC %md
# MAGIC #### How to configure github

# COMMAND ----------

# MAGIC %md
# MAGIC 1. In github, at the top righ corner click on settings, click on developer settings
# MAGIC 2. click on personal access token, click on token and then generate new token(classic)
# MAGIC 3. give token name, choose expiration day,then select scope, in select scope click on repo and other scope
# MAGIC 4. generate token, copy the token.
# MAGIC 5. in the azure databrick workspace page, click on settings at the top right corner, click linked account
# MAGIC 6. under git provider, click git hub and then give tick mark to personal access toekn.
# MAGIC 7. give git provider user email
# MAGIC 8. save the token detail which generated in github.
# MAGIC 9. then save the changes.
# MAGIC 10. then click on workspace at top left, click on repos and create new repo
# MAGIC 11. give git repo url details(click repository in github and click on code, you will get the https url details), give github as gitprovider
# MAGIC and then give repository name, then save it.

# COMMAND ----------


