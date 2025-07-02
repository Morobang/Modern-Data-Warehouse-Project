
# DataForge Pipeline: Modern Data Warehouse Implementation

![Data Pipeline Diagram](https://via.placeholder.com/800x400.png?text=Bronze-Silver-Gold+Architecture)  
*Inspired by [Baraa's Data Warehouse Tutorial](https://www.youtube.com/watch?v=9GVqKuTVANE)*

## 📌 Overview
This project implements a **production-grade data warehouse** using a multi-layered architecture (Bronze/Silver/Gold) to transform raw data into business insights. While inspired by [Data with Baraa's tutorial](https://www.youtube.com/watch?v=9GVqKuTVANE), this implementation includes custom adaptations:
- Real-world dataset integration
- Enhanced data validation framework
- Extended documentation for maintainability

## 🛠️ Tech Stack
| Layer        | Technologies Used |
|--------------|-------------------|
| **Bronze**   | Python, SQL        |
| **Silver**   | PySpark, Pandas   |
| **Gold**     | Star Schema SQL    |
| **Tools**    | Draw.io, Git       |

## 🔄 Project Progress
```mermaid
gantt
    title Project Timeline
    dateFormat  YYYY-MM-DD
    section Phases
    Requirements Analysis   :done,    des1, 2024-01-01, 15d
    Bronze Layer            :active,  des2, 2024-01-16, 30d
    Silver Layer           :         des3, after des2, 30d
    Gold Layer             :         des4, after des3, 20d
```

## 📂 Repository Structure
```
DataForge-Pipeline/
├── bronze/       # Raw data ingestion
│   ├── validator.py
│   └── schemas/
├── silver/       # Cleaned data
│   ├── transformer.py
│   └── tests/
├── gold/         # Analytics models
│   ├── star_schema/
│   └── docs/
└── architecture/
    ├── pipeline.drawio
    └── decisions.md
```

## 💡 Key Differentiators
- **Custom Data Quality Checks**: Added schema validation beyond tutorial scope
- **Production-Ready Documentation**: Detailed runbooks and lineage tracking
- **Adapted Architecture**: Modified layer transitions for our specific data needs

## 📜 Academic Integrity
This project follows **best practices** from:
1. [Data with Baraa's YouTube Tutorial](https://www.youtube.com/watch?v=9GVqKuTVANE)
2. Kimball Group's dimensional modeling principles
3. Microsoft's modern data warehouse guidelines

---
[![LinkedIn](https://img.shields.io/badge/Connect-%230077B5?logo=linkedin)](www.linkedin.com/in/morobang-tshigidimisa-84172b26b)  
📧 **Contact**: morobangtshigidimisa@gmail.com
