
---

## Features

- Basic SQL-like operations:
  - CREATE, INSERT, SELECT, UPDATE, DELETE
- Set operations:
  - UNION, INTERSECT
- Relational operations:
  - INNER JOIN
- File persistence:
  - SAVE/LOAD tables to/from disk

---

## Example Commands

```sql
CREATE TABLE Students
> ID INT
> Name STRING
> DONE

INSERT INTO Students
> 1 Alice

SELECT * FROM Students

UPDATE Students SET Name Bob WHERE ID = 1

DELETE FROM Students WHERE ID = 1

UNION Students Alumni
INTERSECT Students Alumni
JOIN Students Alumni ON ID

SAVE Students
LOAD Students
