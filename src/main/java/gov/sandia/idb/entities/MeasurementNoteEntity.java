package gov.sandia.idb.entities;

public class MeasurementNoteEntity {
    // private int measurementId;
    private int uuid;
    private String csv_field_name;
    private String table_name;
    private String column_name;
    private String note;
    public int getUuid() {
        return uuid;
    }
    public void setUuid(int uuid) {
        this.uuid = uuid;
    }
    public String getCsv_field_name() {
        return csv_field_name;
    }
    public void setCsv_field_name(String csv_field_name) {
        this.csv_field_name = csv_field_name;
    }
    public String getTable_name() {
        return table_name;
    }
    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }
    public String getColumn_name() {
        return column_name;
    }
    public void setColumn_name(String column_name) {
        this.column_name = column_name;
    }
    public String getNote() {
        return note;
    }
    public void setNote(String note) {
        this.note = note;
    }
    

    public static boolean haveEqualValues(final MeasurementNoteEntity first, final MeasurementNoteEntity second) {
        if (first == null) {
            if (second == null) {
                return true;
            } else {
                return false;
            }
        } else if (second == null) {
            return false;
        }

        if (first.getCsv_field_name() == null) {
            if (second.getCsv_field_name() != null) {
                return false;
            }
        } else if (!first.getCsv_field_name().equals(second.getCsv_field_name())) {
            return false;
        }

        if (first.getTable_name() == null) {
            if (second.getTable_name() != null) {
                return false;
            }
        } else if (!first.getTable_name().equals(second.getTable_name())) {
            return false;
        }

        if (first.getColumn_name() == null) {
            if (second.getColumn_name() != null) {
                return false;
            }
        } else if (!first.getColumn_name().equals(second.getColumn_name())) {
            return false;
        }

        if (first.getNote() == null) {
            if (second.getNote() != null) {
                return false;
            }
        } else if (!first.getNote().equals(second.getNote())) {
            return false;
        }
        
        return true;
    }
}
