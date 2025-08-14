# Governance Database 

Pulls all required information from OPENDART API to build a database of KOSPI-listed corporations and their most recent reports regarding executive status and compensation. 

## Outputs 

**exec_df**
<img width="1640" height="637" alt="Image" src="https://github.com/user-attachments/assets/3b92f29a-696b-4c9c-8421-1ca4d016c4b1" />

columns:	Gender, Position, Status, Responsibilities, Relation to Largest Shareholder, Employment Period, is_audit_committee_member, is_auditor	salary, salary_source, salary_type, Stock Owned, Education, Work Experience 

**summary_df**
<img width="1577" height="439" alt="image" src="https://github.com/user-attachments/assets/3f2f94f6-a8a0-47de-bd86-50482f0cd634" />

columns: 	corp_code, corp_name,	Audit Committee,	Audit Committee ODs,	Inside Directors,	Outside Directors,	Other Non-Exec Directors,	Auditors,	Female Voting,	Male Voting,	Voting Directors,	Non Registered,	Total Assets (YYYY)*,	Total Assets (YYYY - 1),	Total Assets (YYYY - 2),	rcept_no

* where YYYY indicates the most recent annual report
