package cis5550.kvs;

public class RowVersioning {
    // private final List<Row> versions = new ArrayList<>();
    private Row latestVersion = null;

    // Adds a new version of a row.
    public void AddNewRowVersion(Row row) {
        Row clonedRow = row.clone();
        // versions.add(clonedRow);
        latestVersion = clonedRow;
    }

    // Retrieves a specific version of a row.
    public Row getVersionOfRow(int version) {
        // if (version > 0 && version <= versions.size()) {
        //     return versions.get(version - 1);
        // }
        return latestVersion;
        // throw new IllegalArgumentException("Invalid version number");
    }

    // Retrieves the latest version of a row.
    public Row getLatestVersionOfRow() {
        if (latestVersion == null) {
            throw new IllegalStateException("No versions available");
        }
        return latestVersion;
    }

    // Returns the current version number.
    public int getCurrentVersionOfRow() {
        if (latestVersion != null)
            return 1; //versions.size();
        return 0;
    }

    // Checks if conditional PUT is valid.
    public boolean checkForConditionalPut(String ifColumn, String equalsQueryParam) {
        if (latestVersion == null) { // 
            return false;
        }
        return equalsQueryParam.equals(latestVersion.get(ifColumn));
    }

    // Retrieves data for a specified column from a specific version.
    public byte[] getColumnData(String column, Integer version) {
        Row row;
        if (version == null) {
            row = latestVersion;
        } else {
            row = getVersionOfRow(version);
        }

        if (row == null) {
            return null;
        }
        return row.getBytes(column);
    }
}
