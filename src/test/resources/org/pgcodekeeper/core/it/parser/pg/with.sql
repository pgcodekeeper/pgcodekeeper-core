WITH q1(x,y) AS (SELECT 1,2) SELECT * FROM q1, q1 AS q2;
WITH q AS (SELECT 'foo' AS x) SELECT x, x IS OF (text) AS is_text FROM q;

WITH RECURSIVE subdepartment AS
(
  SELECT name as root_name, * FROM department WHERE name = 'A'
  UNION ALL
  SELECT sd.root_name, d.* FROM department AS d, subdepartment AS sd
    WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment ORDER BY name;

SELECT count(*) FROM (
    WITH RECURSIVE t(n) AS (
        SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 500
    )
    SELECT * FROM t) AS t WHERE n < (
        SELECT count(*) FROM (
            WITH RECURSIVE t(n) AS (
                   SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 100
                )
            SELECT * FROM t WHERE n < 50000
         ) AS t WHERE n < 100);

WITH q1(x,y) AS (
    SELECT hundred, sum(ten) FROM tenk1 GROUP BY hundred
)
SELECT count(*) FROM q1 WHERE y > (SELECT sum(y)/100 FROM q1 qsub);

with recursive q as (
      select * from department
    union all
      (with x as (select * from q)
       select * from x)
    )
select * from q limit 24;

with recursive q as (
      select * from department
    union all
      (with recursive x as (
           select * from department
         union all
           (select * from q union all select * from x)
        )
       select * from x)
    )
select * from q limit 32;

-- recursive term has sub-UNION
WITH RECURSIVE t(i,j) AS (
	VALUES (1,2)
	UNION ALL
	SELECT t2.i, t.j+1 FROM
		(SELECT 2 AS i UNION ALL SELECT 3 AS i) AS t2
		JOIN t ON (t2.i = t.i+1))

	SELECT * FROM t;

INSERT INTO tree
VALUES (1, NULL), (2, 1), (3,1), (4,2), (5,2), (6,2), (7,3), (8,3),
       (9,4), (10,4), (11,7), (12,7), (13,7), (14, 9), (15,11), (16,11);

--
-- get all paths from "second level" nodes to leaf nodes
--
WITH RECURSIVE t(id, path) AS (
    VALUES(1,ARRAY[]::integer[])
UNION ALL
    SELECT tree.id, t.path || tree.id
    FROM tree JOIN t ON (tree.parent_id = t.id)
)
SELECT t1.*, t2.* FROM t AS t1 JOIN t AS t2 ON
	(t1.path[1] = t2.path[1] AND
	array_upper(t1.path,1) = 1 AND
	array_upper(t2.path,1) > 1)
	ORDER BY t1.id, t2.id;

-- just count 'em
WITH RECURSIVE t(id, path) AS (
    VALUES(1,ARRAY[]::integer[])
UNION ALL
    SELECT tree.id, t.path || tree.id
    FROM tree JOIN t ON (tree.parent_id = t.id)
)
SELECT t1.id, count(t2.*) FROM t AS t1 JOIN t AS t2 ON
	(t1.path[1] = t2.path[1] AND
	array_upper(t1.path,1) = 1 AND
	array_upper(t2.path,1) > 1)
	GROUP BY t1.id
	ORDER BY t1.id;

-- this variant tickled a whole-row-variable bug in 8.4devel
WITH RECURSIVE t(id, path) AS (
    VALUES(1/*,ARRAY[]::integer[]*/)
UNION ALL
    SELECT tree.id, t.path || tree.id
    FROM tree JOIN t ON (tree.parent_id = t.id)
)
SELECT t1.id, t2.path, t2 FROM t AS t1 JOIN t AS t2 ON
(t1.id=t2.id);

with recursive search_graph(f, t, label, path, cycle) as (
	select *, array[row(g.f, g.t)], false from graph g
	union all
	select g.*, path || row(g.f, g.t), row(g.f, g.t) = any(path)
	from graph g, search_graph sg
	where g.f = sg.t and not cycle
)
select * from search_graph;

-- ordering by the path column has same effect as SEARCH DEPTH FIRST
with recursive search_graph(f, t, label, path, cycle) as (
	select *, array[row(g.f, g.t)], false from graph g
	union all
	select g.*, path || row(g.f, g.t), row(g.f, g.t) = any(path)
	from graph g, search_graph sg
	where g.f = sg.t and not cycle
)
select * from search_graph order by path;

--
-- test multiple WITH queries
--
WITH RECURSIVE
  y (id) AS (VALUES (1)),
  x (id) AS (SELECT * FROM y UNION ALL SELECT id+1 FROM x WHERE id < 5)
SELECT * FROM x;

-- forward reference OK
WITH RECURSIVE
    x(id) AS (SELECT * FROM y UNION ALL SELECT id+1 FROM x WHERE id < 5),
    y(id) AS (values (1))
 SELECT * FROM x;

WITH RECURSIVE
   x(id) AS
     (VALUES (1) UNION ALL SELECT id+1 FROM x WHERE id < 5),
   y(id) AS
     (VALUES (1) UNION ALL SELECT id+1 FROM y WHERE id < 10)
 SELECT y.*, x.* FROM y LEFT JOIN x USING (id);

WITH RECURSIVE
   x(id) AS
     (VALUES (1) UNION ALL SELECT id+1 FROM x WHERE id < 5),
   y(id) AS
     (VALUES (1) UNION ALL SELECT id+1 FROM x WHERE id < 10)
 SELECT y.*, x.* FROM y LEFT JOIN x USING (id);

WITH RECURSIVE
   x(id) AS
     (SELECT 1 UNION ALL SELECT id+1 FROM x WHERE id < 3 ),
   y(id) AS
     (SELECT * FROM x UNION ALL SELECT * FROM x),
   z(id) AS
     (SELECT * FROM x UNION ALL SELECT id+1 FROM z WHERE id < 10)
 SELECT * FROM z;

WITH RECURSIVE
   x(id) AS
     (SELECT 1 UNION ALL SELECT id+1 FROM x WHERE id < 3 ),
   y(id) AS
     (SELECT * FROM x UNION ALL SELECT * FROM x),
   z(id) AS
     (SELECT * FROM y UNION ALL SELECT id+1 FROM z WHERE id < 10)
 SELECT * FROM z;

WITH t AS (
	SELECT a FROM y
)
INSERT INTO y
SELECT a+20 FROM t RETURNING *;

WITH t AS (
	SELECT a FROM y
)
UPDATE y SET a = y.a-10 FROM t WHERE y.a > 20 AND t.a = y.a RETURNING y.a;

WITH RECURSIVE t(a) AS (
	SELECT 11
	UNION ALL
	SELECT a+1 FROM t WHERE a < 50
)
DELETE FROM y USING t WHERE t.a = y.a RETURNING y.a;

WITH RECURSIVE x(n) AS (SELECT 1 INTERSECT SELECT n+1 FROM x) SELECT * FROM x;
WITH RECURSIVE x(n) AS (SELECT 1 INTERSECT ALL SELECT n+1 FROM x) SELECT * FROM x;
WITH RECURSIVE x(n) AS (SELECT 1 EXCEPT SELECT n+1 FROM x) SELECT * FROM x;
WITH RECURSIVE x(n) AS (SELECT 1 EXCEPT ALL SELECT n+1 FROM x) SELECT * FROM x;
WITH RECURSIVE x(n) AS (SELECT n FROM x) SELECT * FROM x;

-- recursive term in the left hand side (strictly speaking, should allow this)
WITH RECURSIVE x(n) AS (SELECT n FROM x UNION ALL SELECT 1)
	SELECT * FROM x;

-- LEFT JOIN

WITH RECURSIVE x(n) AS (SELECT a FROM y WHERE a = 1
	UNION ALL
	SELECT x.n+1 FROM y LEFT JOIN x ON x.n = y.a WHERE n < 10)
SELECT * FROM x;

-- RIGHT JOIN
WITH RECURSIVE x(n) AS (SELECT a FROM y WHERE a = 1
	UNION ALL
	SELECT x.n+1 FROM x RIGHT JOIN y ON x.n = y.a WHERE n < 10)
SELECT * FROM x;

-- FULL JOIN
WITH RECURSIVE x(n) AS (SELECT a FROM y WHERE a = 1
	UNION ALL
	SELECT x.n+1 FROM x FULL JOIN y ON x.n = y.a WHERE n < 10)
SELECT * FROM x;

-- subquery
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x
                          WHERE n IN (SELECT * FROM x))
  SELECT * FROM x;

-- aggregate functions
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT count(*) FROM x)
  SELECT * FROM x;

WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT sum(n) FROM x)
  SELECT * FROM x;

-- ORDER BY
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x ORDER BY 1)
  SELECT * FROM x;

-- LIMIT/OFFSET
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x LIMIT 10 OFFSET 1)
  SELECT * FROM x;

-- FOR UPDATE
WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x FOR UPDATE)
  SELECT * FROM x;

-- target list has a recursive query name
WITH RECURSIVE x(id) AS (values (1)
    UNION ALL
    SELECT (SELECT * FROM x) FROM x WHERE id < 5
) SELECT * FROM x;

-- mutual recursive query (not implemented)
WITH RECURSIVE
  x (id) AS (SELECT 1 UNION ALL SELECT id+1 FROM y WHERE id < 5),
  y (id) AS (SELECT 1 UNION ALL SELECT id+1 FROM x WHERE id < 5)
SELECT * FROM x;

-- non-linear recursion is not allowed
WITH RECURSIVE foo(i) AS
    (values (1)
    UNION ALL
       (SELECT i+1 FROM foo WHERE i < 10
          UNION ALL
       SELECT i+1 FROM foo WHERE i < 5)
) SELECT * FROM foo;

WITH RECURSIVE foo(i) AS
    (values (1)
    UNION ALL
	   SELECT * FROM
       (SELECT i+1 FROM foo WHERE i < 10
          UNION ALL
       SELECT i+1 FROM foo WHERE i < 5) AS t
) SELECT * FROM foo;

WITH RECURSIVE foo(i) AS
    (values (1)
    UNION ALL
       (SELECT i+1 FROM foo WHERE i < 10
          EXCEPT
       SELECT i+1 FROM foo WHERE i < 5)
) SELECT * FROM foo;

WITH RECURSIVE foo(i) AS
    (values (1)
    UNION ALL
       (SELECT i+1 FROM foo WHERE i < 10
          INTERSECT
       SELECT i+1 FROM foo WHERE i < 5)
) SELECT * FROM foo;

-- Wrong type induced from non-recursive term
WITH RECURSIVE foo(i) AS
   (SELECT i FROM (VALUES(1),(2)) t(i)
   UNION ALL
   SELECT (i+1)::numeric(10,0) FROM foo WHERE i < 10)
SELECT * FROM foo;

-- rejects different typmod, too (should we allow this?)
WITH RECURSIVE foo(i) AS
   (SELECT i::numeric(3,0) FROM (VALUES(1),(2)) t(i)
   UNION ALL
   SELECT (i+1)::numeric(10,0) FROM foo WHERE i < 10)
SELECT * FROM foo;

with cte(foo) as ( values(42) ) values((select foo from cte));
with cte(foo) as ( select 42 ) select * from ((select foo from cte)) q;  -- ambiguity

-- test CTE referencing an outer-level variable (to see that changed-parameter
-- signaling still works properly after fixing this bug)
select ( with cte(foo) as ( values(f1) )
         select (select foo from cte) )
from int4_tbl;

select ( with cte(foo) as ( values(f1) )
          values((select foo from cte)) )
from int4_tbl;

--
-- test for nested-recursive-WITH bug
--
WITH RECURSIVE t(j) AS (
    WITH RECURSIVE s(i) AS (
        VALUES (1)
        UNION ALL
        SELECT i+1 FROM s WHERE i < 10
    )
    SELECT i FROM s
    UNION ALL
    SELECT j+1 FROM t WHERE j < 10
)
SELECT * FROM t;

--
-- test WITH attached to intermediate-level set operation
--

WITH outermost(x) AS (
  SELECT 1
  UNION (WITH innermost as (SELECT 2)
         SELECT * FROM innermost
         UNION SELECT 3)
)
SELECT * FROM outermost ORDER BY 1;

WITH outermost(x) AS (
  SELECT 1
  UNION (WITH innermost as (SELECT 2)
         SELECT * FROM outermost  -- fail
         UNION SELECT * FROM innermost)
)
SELECT * FROM outermost ORDER BY 1;

WITH RECURSIVE outermost(x) AS (
  SELECT 1
  UNION (WITH innermost as (SELECT 2)
         SELECT * FROM outermost
         UNION SELECT * FROM innermost)
)
SELECT * FROM outermost ORDER BY 1;

WITH RECURSIVE outermost(x) AS (
  WITH innermost as (SELECT 2 FROM outermost) -- fail
    SELECT * FROM innermost
    UNION SELECT * from outermost
)
SELECT * FROM outermost ORDER BY 1;

--
-- This test will fail with the old implementation of PARAM_EXEC parameter
-- assignment, because the "q1" Var passed down to A's targetlist subselect
-- looks exactly like the "A.id" Var passed down to C's subselect, causing
-- the old code to give them the same runtime PARAM_EXEC slot.  But the
-- lifespans of the two parameters overlap, thanks to B also reading A.
--

with
A as ( select q2 as id, (select q1) as x from int8_tbl ),
B as ( select id, row_number() over (partition by id) as r from A ),
C as ( select A.id, array(select B.id from B where B.id = A.id) from A )
select * from C;

--
-- Test CTEs read in non-initialization orders
--

WITH RECURSIVE
  tab(id_key,link) AS (VALUES (1,17), (2,17), (3,17), (4,17), (6,17), (5,17)),
  iter (id_key, row_type, link) AS (
      SELECT 0, 'base', 17
    UNION ALL (
      WITH remaining(id_key, row_type, link, min) AS (
        SELECT tab.id_key, 'true'::text, iter.link, MIN(tab.id_key) OVER ()
        FROM tab INNER JOIN iter USING (link)
        WHERE tab.id_key > iter.id_key
      ),
      first_remaining AS (
        SELECT id_key, row_type, link
        FROM remaining
        WHERE id_key=min
      ),
      effect AS (
        SELECT tab.id_key, 'new'::text, tab.link
        FROM first_remaining e INNER JOIN tab ON e.id_key=tab.id_key
        WHERE e.row_type = 'false'
      )
      SELECT * FROM first_remaining
      UNION ALL SELECT * FROM effect
    )
  )
SELECT * FROM iter;

WITH RECURSIVE
  tab(id_key,link) AS (VALUES (1,17), (2,17), (3,17), (4,17), (6,17), (5,17)),
  iter (id_key, row_type, link) AS (
      SELECT 0, 'base', 17
    UNION (
      WITH remaining(id_key, row_type, link, min) AS (
        SELECT tab.id_key, 'true'::text, iter.link, MIN(tab.id_key) OVER ()
        FROM tab INNER JOIN iter USING (link)
        WHERE tab.id_key > iter.id_key
      ),
      first_remaining AS (
        SELECT id_key, row_type, link
        FROM remaining
        WHERE id_key=min
      ),
      effect AS (
        SELECT tab.id_key, 'new'::text, tab.link
        FROM first_remaining e INNER JOIN tab ON e.id_key=tab.id_key
        WHERE e.row_type = 'false'
      )
      SELECT * FROM first_remaining
      UNION ALL SELECT * FROM effect
    )
  )
SELECT * FROM iter;

--
-- Data-modifying statements in WITH
--

-- INSERT ... RETURNING
WITH t AS (
    INSERT INTO y
    VALUES
        (11),
        (12),
        (13),
        (14),
        (15),
        (16),
        (17),
        (18),
        (19),
        (20)
    RETURNING *
)
SELECT * FROM t;

-- UPDATE ... RETURNING
WITH t AS (
    UPDATE y
    SET a=a+1
    RETURNING *
)
SELECT * FROM t;

-- DELETE ... RETURNING
WITH t AS (
    DELETE FROM y
    WHERE a <= 10
    RETURNING *
)
SELECT * FROM t;

-- forward reference
WITH RECURSIVE t AS (
	INSERT INTO y
		SELECT a+5 FROM t2 WHERE a > 5
	RETURNING *
), t2 AS (
	UPDATE y SET a=a-11 RETURNING *
)
SELECT * FROM t
UNION ALL
SELECT * FROM t2;

WITH t AS (
	DELETE FROM y RETURNING *
)
SELECT * FROM t;

WITH t1 AS ( DELETE FROM bug6051 RETURNING * )
INSERT INTO bug6051 SELECT * FROM t1;

-- a truly recursive CTE in the same list
WITH RECURSIVE t(a) AS (
	SELECT 0
		UNION ALL
	SELECT a+1 FROM t WHERE a+1 < 5
), t2 as (
	INSERT INTO y
		SELECT * FROM t RETURNING *
)
SELECT * FROM t2 JOIN y USING (a) ORDER BY a;

-- data-modifying WITH in a modifying statement
WITH t AS (
    DELETE FROM y
    WHERE a <= 10
    RETURNING *
)
INSERT INTO y SELECT -a FROM t RETURNING *;

-- check that WITH query is run to completion even if outer query isn't
WITH t AS (
    UPDATE y SET a = a * 100 RETURNING *
)
SELECT * FROM t LIMIT 10;

WITH t AS (
    INSERT INTO withz SELECT i, 'insert'
    FROM generate_series(0, 16) i
    ON CONFLICT (k) DO UPDATE SET v = withz.v || ', now update'
    RETURNING *
)
SELECT * FROM t JOIN y ON t.k = y.a ORDER BY a, k;

-- Test EXCLUDED.* reference within CTE
WITH aa AS (
    INSERT INTO withz VALUES(1, 5) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
    WHERE withz.k != EXCLUDED.k
    RETURNING *
)
SELECT * FROM aa;

--
-- Ensure subqueries within the update clause work, even if they
-- reference outside values
--
WITH aa AS (SELECT 1 a, 2 b)
INSERT INTO withz VALUES(1, 'insert')
ON CONFLICT (k) DO UPDATE SET v = (SELECT b || ' update' FROM aa WHERE a = 1 LIMIT 1);
WITH aa AS (SELECT 1 a, 2 b)
INSERT INTO withz VALUES(1, 'insert')
ON CONFLICT (k) DO UPDATE SET v = ' update' WHERE withz.k = (SELECT a FROM aa);
WITH aa AS (SELECT 1 a, 2 b)
INSERT INTO withz VALUES(1, 'insert')
ON CONFLICT (k) DO UPDATE SET v = (SELECT b || ' update' FROM aa WHERE a = 1 LIMIT 1);
WITH aa AS (SELECT 'a' a, 'b' b UNION ALL SELECT 'a' a, 'b' b)
INSERT INTO withz VALUES(1, 'insert')
ON CONFLICT (k) DO UPDATE SET v = (SELECT b || ' update' FROM aa WHERE a = 'a' LIMIT 1);
WITH aa AS (SELECT 1 a, 2 b)
INSERT INTO withz VALUES(1, (SELECT b || ' insert' FROM aa WHERE a = 1 ))
ON CONFLICT (k) DO UPDATE SET v = (SELECT b || ' update' FROM aa WHERE a = 1 LIMIT 1);

-- Update a row more than once, in different parts of a wCTE. That is
-- an allowed, presumably very rare, edge case, but since it was
-- broken in the past, having a test seems worthwhile.
WITH simpletup AS (
  SELECT 2 k, 'Green' v),
upsert_cte AS (
  INSERT INTO withz VALUES(2, 'Blue') ON CONFLICT (k) DO
    UPDATE SET (k, v) = (SELECT k, v FROM simpletup WHERE simpletup.k = withz.k)
    RETURNING k, v)
INSERT INTO withz VALUES(2, 'Red') ON CONFLICT (k) DO
UPDATE SET (k, v) = (SELECT k, v FROM upsert_cte WHERE upsert_cte.k = withz.k)
RETURNING k, v;

-- check that run to completion happens in proper ordering

WITH RECURSIVE t1 AS (
  INSERT INTO y SELECT * FROM y RETURNING *
), t2 AS (
  INSERT INTO yy SELECT * FROM t1 RETURNING *
)
SELECT 1;

WITH RECURSIVE t1 AS (
  INSERT INTO yy SELECT * FROM t2 RETURNING *
), t2 AS (
  INSERT INTO y SELECT * FROM y RETURNING *
)
SELECT 1;

WITH t AS (
    INSERT INTO y
    VALUES
        (21),
        (22),
        (23)
    RETURNING *
)
SELECT * FROM t;

WITH t AS (
    INSERT INTO y
    VALUES
        (31),
        (32),
        (33)
    RETURNING *
)
SELECT * FROM t LIMIT 1;

WITH t AS (
    INSERT INTO y
    VALUES
        (41),
        (42),
        (43)
    RETURNING *
)
SELECT * FROM t;

WITH rcte AS ( SELECT sum(id) AS totalid FROM parent )
UPDATE parent SET id = id + totalid FROM rcte;

WITH wcte AS ( INSERT INTO child1 VALUES ( 42, 'new' ) RETURNING id AS newid )
UPDATE parent SET id = id + newid FROM wcte;

WITH rcte AS ( SELECT max(id) AS maxid FROM parent )
DELETE FROM parent USING rcte WHERE id = maxid;

WITH wcte AS ( INSERT INTO child2 VALUES ( 42, 'new2' ) RETURNING id AS newid )
DELETE FROM parent USING wcte WHERE id = newid;

-- check EXPLAIN VERBOSE for a wCTE with RETURNING

EXPLAIN (VERBOSE, COSTS OFF)
WITH wcte AS ( INSERT INTO int8_tbl VALUES ( 42, 47 ) RETURNING q2 )
DELETE FROM a USING wcte WHERE aa = q2;

-- error cases

-- data-modifying WITH tries to use its own output
WITH RECURSIVE t AS (
	INSERT INTO y
		SELECT * FROM t
)
VALUES(FALSE);

-- no RETURNING in a referenced data-modifying WITH
WITH t AS (
	INSERT INTO y VALUES(0)
)
SELECT * FROM t;

-- data-modifying WITH allowed only at the top level
SELECT * FROM (
	WITH t AS (UPDATE y SET a=a+1 RETURNING *)
	SELECT * FROM t
) ss;

WITH t AS (
	INSERT INTO y VALUES(0)
)
VALUES(FALSE);

-- check that parser lookahead for WITH doesn't cause any odd behavior
with ordinality as (select 1 as x) select * from ordinality;

-- check sane response to attempt to modify CTE relation
WITH test AS (SELECT 42) INSERT INTO test VALUES (1);

-- check response to attempt to modify table with same name as a CTE (perhaps
-- surprisingly it works, because CTEs don't hide tables from data-modifying
-- statements)
with test as (select 42) insert into test select * from test;
select * from test;

with recursive search_graph(f, t, label) as (
    select * from graph0 g
    union all
    select g.*
    from graph0 g, search_graph sg
    where g.f = sg.t
) search depth first by f, t set seq
select * from search_graph order by seq;

with recursive search_graph(f, t, label) as (
    select * from graph0 g
    union all
    select g.*
    from graph0 g, search_graph sg
    where g.f = sg.t
) search breadth first by f, t set seq
select * from search_graph order by seq;
