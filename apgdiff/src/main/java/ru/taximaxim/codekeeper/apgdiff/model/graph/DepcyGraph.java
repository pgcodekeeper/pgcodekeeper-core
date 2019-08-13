package ru.taximaxim.codekeeper.apgdiff.model.graph;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;

import cz.startnet.utils.pgdiff.schema.AbstractColumn;
import cz.startnet.utils.pgdiff.schema.AbstractConstraint;
import cz.startnet.utils.pgdiff.schema.AbstractIndex;
import cz.startnet.utils.pgdiff.schema.AbstractPgTable;
import cz.startnet.utils.pgdiff.schema.AbstractPgTable.Inherits;
import cz.startnet.utils.pgdiff.schema.AbstractTable;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.IStatementContainer;
import cz.startnet.utils.pgdiff.schema.PartitionPgTable;
import cz.startnet.utils.pgdiff.schema.PgColumn;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import ru.taximaxim.codekeeper.apgdiff.Log;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class DepcyGraph {

    private final DirectedGraph<PgStatement, DefaultEdge> graph =
            new SimpleDirectedGraph<>(DefaultEdge.class);

    private final EdgeReversedGraph<PgStatement, DefaultEdge> reversedGraph =
            new EdgeReversedGraph<>(graph);

    /**
     * Направление связей в графе:<br>
     * зависящий объект → зависимость <br>
     * source → target
     */
    public DirectedGraph<PgStatement, DefaultEdge> getGraph() {
        return graph;
    }

    public EdgeReversedGraph<PgStatement, DefaultEdge> getReversedGraph(){
        return reversedGraph;
    }

    private final PgDatabase db;

    /**
     * Copied database, graph source.<br>
     * <b>Do not modify</b> any elements in this as it will break
     * HashSets/HashMaps and with them the generated graph.
     */
    public PgDatabase getDb(){
        return db;
    }

    public DepcyGraph(PgDatabase graphSrc) {
        db = (PgDatabase) graphSrc.deepCopy();
        create();
    }

    private void create() {
        graph.addVertex(db);

        // first pass: object tree
        db.getDescendants().flatMap(AbstractTable::columnAdder).forEach(st -> {
            graph.addVertex(st);
            graph.addEdge(st, st.getParent());
        });


        // second pass: dependency graph
        db.getDescendants().flatMap(AbstractTable::columnAdder).forEach(st -> {
            processDeps(st);
            if (st.getStatementType() == DbObjType.CONSTRAINT) {
                createFkeyToUnique((AbstractConstraint)st);
            } else if (st.getStatementType() == DbObjType.COLUMN
                    && st.isPostgres()) {
                PgColumn col = (PgColumn) st;
                PgStatement tbl = col.getParent();
                if (st.getParent() instanceof PartitionPgTable) {
                    createChildColToPartTblCol((PartitionPgTable) tbl, col);
                } else {
                    // Creating the connection between the column of a inherit
                    // table and the columns of its child tables.

                    AbstractColumn parentTblCol = col.getParentCol((AbstractPgTable) tbl);
                    if (parentTblCol != null) {
                        graph.addEdge(col, parentTblCol);
                    }
                }
            }
        });
    }

    private void processDeps(PgStatement st) {
        for (GenericColumn dep : st.getDeps()) {
            PgStatement depSt = dep.getStatement(db);
            if (depSt != null && !st.equals(depSt)) {
                graph.addEdge(st, depSt);
            }
        }
    }

    /**
     * The only way to find this depcy is to compare refcolumns against all existing unique
     * contraints/keys in reftable.
     * Unfortunately they might not exist at the stage where {@link PgStatement#getDeps()}
     * are populated so we have to defer their lookup until here.
     */
    private void createFkeyToUnique(AbstractConstraint con) {
        Set<String> refs = con.getForeignColumns();
        GenericColumn refTable = con.getForeignTable();
        if (!refs.isEmpty() && refTable != null) {
            PgStatement cont = refTable.getStatement(db);
            if (cont instanceof IStatementContainer) {
                IStatementContainer c = (IStatementContainer) cont;
                for (AbstractConstraint refCon : c.getConstraints()) {
                    if ((refCon.isPrimaryKey() || refCon.isUnique()) && refs.equals(refCon.getColumns())) {
                        graph.addEdge(con, refCon);
                    }
                }
                for (AbstractIndex refInd : c.getIndexes()) {
                    if (refInd.isUnique() && refs.equals(refInd.getColumns())) {
                        graph.addEdge(con, refInd);
                    }
                }
            }
        }
    }

    /**
     * Creates the connection between the column of a partitioned table and the
     * columns of its sections (child tables).
     * <br />
     * Partitioned tables cannot use the inheritance mechanism, as in simple tables.
     */
    private void createChildColToPartTblCol(PartitionPgTable tbl, PgColumn col) {
        for (Inherits in : tbl.getInherits()) {
            PgStatement parentTbl = new GenericColumn(in.getKey(), in.getValue(),
                    DbObjType.TABLE).getStatement(db);
            if (parentTbl == null) {
                Log.log(Log.LOG_ERROR, "There is no such partitioned table as: "
                        + in.getQualifiedName());
                continue;
            }

            if (parentTbl instanceof PartitionPgTable) {
                createChildColToPartTblCol((PartitionPgTable) parentTbl, col);
            } else {
                String colName = col.getName();
                AbstractColumn parentCol = ((AbstractTable) parentTbl).getColumn(colName);
                if (parentCol != null) {
                    graph.addEdge(col, parentCol);
                } else {
                    Log.log(Log.LOG_ERROR, "The parent '" + in.getQualifiedName()
                    + '.' + colName + "' column for '" + col.getSchemaName()
                    + '.' + col.getParent().getName()
                    + '.' + colName + "' column is missed.");
                }
            }
        }
    }

    public void addCustomDepcies(List<Entry<PgStatement, PgStatement>> depcies) {
        for (Entry<PgStatement, PgStatement> depcy : depcies) {
            graph.addEdge(depcy.getKey(), depcy.getValue());
        }
    }
}
