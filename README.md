# CustomerVoice Analytics
AI-powered customer support analytics platform with NLP and PostgreSQL
## Supported Data Formats

CX Insights Lab works with ANY customer support data! We support:

### File Formats
- CSV (.csv)
- Excel (.xlsx, .xls)

### Required Fields
Your data must have AT LEAST:
- **Date field**: When ticket was created (any date format)
- **Text field**: Ticket content/description

### Optional Fields
We'll auto-detect these if present:
- Ticket ID
- Category/Type
- Priority/Severity
- Channel/Source
- Customer info

### We Handle Messy Data!
Don't worry about:
- ❌ Different column names → We auto-map
- ❌ Multiple date formats → We parse them all
- ❌ HTML/special characters → We clean them
- ❌ Missing values → We handle them
- ❌ Duplicates → We remove them

###  Tested With:
- ✅ Zendesk exports
- ✅ Freshdesk exports
- ✅ Jira Service Desk
- ✅ Custom CSV files
- ✅ Kaggle datasets
- ✅ Twitter support data
