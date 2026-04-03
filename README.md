# Agency-CRM-DataMigration

Script migration dữ liệu vào CRM database (Google Apps Script).

## Scope
- Chỉ gồm code migration deploy từ Apps Script project riêng.
- Không chứa dữ liệu local database.

## Deploy
```bash
clasp push
clasp deploy
```

## CI/CD Secrets Required
- `CLASPRC_JSON`
- `SCRIPT_ID`
- `DEPLOY_ID`

