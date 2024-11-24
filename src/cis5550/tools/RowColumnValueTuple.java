package cis5550.tools;

public class RowColumnValueTuple {

    private final String row;
    private final String column;
    private final String value;

    public RowColumnValueTuple(String row, String column, String value) {
        this.row = row;
        this.column = column;
        this.value = value;
    }

    public String getRow() {
        return row;
    }

    public String getColumn() {
        return column;
    }

    public String getValue() {
        return value;
    }
}
