package ru.taximaxim.codekeeper.core.model.difftree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import ru.taximaxim.codekeeper.core.model.difftree.TreeElement.DiffSide;

public class CompareTreeTest {

    private static final int N = 1000;

    private static List<TreeElement> list;

    @BeforeAll
    public static void beforeTest() {
        list = new ArrayList<>(N * DiffSide.values().length * DbObjType.values().length);
        for (DiffSide side : DiffSide.values()) {
            for (DbObjType type : DbObjType.values()) {
                for (int i = 0; i < N; ++i) {
                    list.add(new TreeElement("testel_" + i, type, side));
                }
            }
        }
        Collections.shuffle(list);
    }

    @Test
    public void test() {
        DiffSide prevSide = DiffSide.LEFT;
        DbObjType prevType = DbObjType.values()[DbObjType.values().length - 1];

        Collections.sort(list, new CompareTree());
        for (TreeElement el : list) {
            if (el.getSide() == DiffSide.LEFT
                    && prevSide != DiffSide.LEFT) {
                Assertions.fail("Side left is after another type");
            }

            int res = el.getType().ordinal() - prevType.ordinal();

            if (el.getSide() == DiffSide.RIGHT
                    && prevSide == DiffSide.BOTH
                    && res <= 0) {
                Assertions.fail("Side wrong");
            }
            if (el.getSide() == DiffSide.LEFT) {
                res = -res;
            }
            if (res < 0) {
                Assertions.fail("Element type is less than expected");
            }
            prevSide = el.getSide();
            prevType = el.getType();
        }
    }
}
