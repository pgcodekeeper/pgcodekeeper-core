package ru.taximaxim.codekeeper.core.parsers.antlr.statements;

import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.parsers.antlr.QNameParser;
import ru.taximaxim.codekeeper.core.parsers.antlr.SQLParser.Character_stringContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.SQLParser.Create_type_statementContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.SQLParser.Table_column_definitionContext;
import ru.taximaxim.codekeeper.core.schema.AbstractColumn;
import ru.taximaxim.codekeeper.core.schema.AbstractSchema;
import ru.taximaxim.codekeeper.core.schema.PgColumn;
import ru.taximaxim.codekeeper.core.schema.PgDatabase;
import ru.taximaxim.codekeeper.core.schema.PgType;
import ru.taximaxim.codekeeper.core.schema.PgType.PgTypeForm;

public class CreateType extends ParserAbstract {

    private final Create_type_statementContext ctx;
    public CreateType(Create_type_statementContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        String name = QNameParser.getFirstName(ids);
        AbstractSchema schema = getSchemaSafe(ids);
        PgTypeForm form = PgTypeForm.SHELL;
        if (ctx.RANGE() != null) {
            form = PgTypeForm.RANGE;
        } else if (ctx.ENUM() != null) {
            form = PgTypeForm.ENUM;
        } else if (ctx.AS() != null) {
            form = PgTypeForm.COMPOSITE;
        } else if (ctx.INPUT() != null) {
            form = PgTypeForm.BASE;
        }
        PgType type = null;
        PgType newType = null;
        if (form == PgTypeForm.BASE && schema != null) {
            type = (PgType) schema.getType(name);
            if (type != null && type.getForm() != PgTypeForm.SHELL) {
                throw new IllegalStateException("Duplicate type but existing is not SHELL type!");
            }
        }
        if (type == null) {
            type = new PgType(name, form);
            newType = type;
        }

        for (Table_column_definitionContext attr : ctx.attrs) {
            addAttr(attr, type);
        }
        for (Character_stringContext enume : ctx.enums) {
            type.addEnum(enume.getText());
        }
        if (ctx.subtype_name != null) {
            type.setSubtype(getTypeName(ctx.subtype_name));
            addPgTypeDepcy(ctx.subtype_name, type);
        }
        if (ctx.subtype_operator_class != null) {
            type.setSubtypeOpClass(getFullCtxText(ctx.subtype_operator_class));
        }
        if (ctx.collation != null) {
            type.setCollation(getFullCtxText(ctx.collation));
        }
        if (ctx.canonical_function != null) {
            type.setCanonical(getFullCtxText(ctx.canonical_function));
            addDepSafe(type, getIdentifiers(ctx.canonical_function), DbObjType.FUNCTION, true);
        }
        if (ctx.subtype_diff_function != null) {
            type.setSubtypeDiff(getFullCtxText(ctx.subtype_diff_function));
            addDepSafe(type, getIdentifiers(ctx.subtype_diff_function), DbObjType.FUNCTION, true);
        }
        if (ctx.multirange_name != null) {
            type.setMultirange(ctx.multirange_name.getText());
            addPgTypeDepcy(ctx.multirange_name, type);
        }
        if (ctx.input_function != null) {
            type.setInputFunction(getFullCtxText(ctx.input_function));
            addDepSafe(type, getIdentifiers(ctx.input_function), DbObjType.FUNCTION, true);
        }
        if (ctx.output_function != null) {
            type.setOutputFunction(getFullCtxText(ctx.output_function));
            addDepSafe(type, getIdentifiers(ctx.output_function), DbObjType.FUNCTION, true);
        }
        if (ctx.receive_function != null) {
            type.setReceiveFunction(getFullCtxText(ctx.receive_function));
            addDepSafe(type, getIdentifiers(ctx.receive_function), DbObjType.FUNCTION, true);
        }
        if (ctx.send_function != null) {
            type.setSendFunction(getFullCtxText(ctx.send_function));
            addDepSafe(type, getIdentifiers(ctx.send_function), DbObjType.FUNCTION, true);
        }
        if (ctx.type_modifier_input_function != null) {
            type.setTypmodInputFunction(getFullCtxText(ctx.type_modifier_input_function));
            addDepSafe(type, getIdentifiers(ctx.type_modifier_input_function), DbObjType.FUNCTION, true);
        }
        if (ctx.type_modifier_output_function != null) {
            type.setTypmodOutputFunction(getFullCtxText(ctx.type_modifier_output_function));
            addDepSafe(type, getIdentifiers(ctx.type_modifier_output_function), DbObjType.FUNCTION, true);
        }
        if (ctx.analyze_function != null) {
            type.setAnalyzeFunction(getFullCtxText(ctx.analyze_function));
            addDepSafe(type, getIdentifiers(ctx.analyze_function), DbObjType.FUNCTION, true);
        }
        if (ctx.subscript_function != null) {
            type.setSubscriptFunction(getFullCtxText(ctx.subscript_function));
            addDepSafe(type, getIdentifiers(ctx.subscript_function), DbObjType.FUNCTION, true);
        }
        if (ctx.internallength != null) {
            type.setInternalLength(getFullCtxText(ctx.internallength));
        }
        List<TerminalNode> variable = ctx.VARIABLE();
        if (!variable.isEmpty()) {
            type.setInternalLength(variable.get(0).getText());
        }
        type.setPassedByValue(!ctx.PASSEDBYVALUE().isEmpty());
        if (ctx.alignment != null) {
            type.setAlignment(getFullCtxText(ctx.alignment));
        }
        if (ctx.storage != null) {
            type.setStorage(ctx.storage.getText());
        }
        if (ctx.like_type != null) {
            type.setLikeType(getFullCtxText(ctx.like_type));
        }
        if (ctx.category != null) {
            type.setCategory(ctx.category.getText());
        }
        if (ctx.preferred != null) {
            type.setPreferred(getFullCtxText(ctx.preferred));
        }
        if (ctx.default_value != null) {
            type.setDefaultValue(ctx.default_value.getText());
        }
        if (ctx.element != null) {
            type.setElement(getTypeName(ctx.element));
            addPgTypeDepcy(ctx.element, type);
        }
        if (ctx.delimiter != null) {
            type.setDelimiter(ctx.delimiter.getText());
        }
        if (ctx.collatable != null) {
            type.setCollatable(getFullCtxText(ctx.collatable));
        }
        if (newType != null) {
            // add only newly created type, not a filled SHELL that was added before
            addSafe(schema, type, ids);
        }
    }

    private void addAttr(Table_column_definitionContext colCtx, PgType type) {
        AbstractColumn col = new PgColumn(colCtx.identifier().getText());
        col.setType(getTypeName(colCtx.data_type()));
        addPgTypeDepcy(colCtx.data_type(), type);
        if (colCtx.collate_identifier() != null) {
            col.setCollation(getFullCtxText(colCtx.collate_identifier().collation));
        }
        type.addAttr(col);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TYPE, getIdentifiers(ctx.name));
    }
}
