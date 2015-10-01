package eu.comsode.unifiedviews.plugins.transformer.generatedtorelational;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.opendatanode.plugins.extractor.ckan.relational.CatalogApiConfig;
import org.opendatanode.plugins.extractor.ckan.relational.DatabaseHelper;
import org.opendatanode.plugins.extractor.ckan.relational.Dataset;
import org.opendatanode.plugins.extractor.ckan.relational.DatastoreSearchResult;
import org.opendatanode.plugins.extractor.ckan.relational.Record;
import org.opendatanode.plugins.extractor.ckan.relational.RelationalFromCkanHelper;
import org.opendatanode.plugins.extractor.ckan.relational.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.relational.WritableRelationalDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dataunit.resource.ResourceHelpers;
import eu.unifiedviews.helpers.dpu.config.ConfigHistory;
import eu.unifiedviews.helpers.dpu.context.ContextUtils;
import eu.unifiedviews.helpers.dpu.exec.AbstractDpu;
import eu.unifiedviews.helpers.dpu.extension.ExtensionInitializer;
import eu.unifiedviews.helpers.dpu.extension.faulttolerance.FaultTolerance;

/**
 * Main data processing unit class.
 */
@DPU.AsTransformer
public class GeneratedToRelational extends AbstractDpu<GeneratedToRelationalConfig_V1> {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratedToRelational.class);

    private static final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final SimpleDateFormat intFmt = new SimpleDateFormat("yyyyMMdd");

    public static final String CONFIGURATION_SECRET_TOKEN = "org.opendatanode.CKAN.secret.token";

    public static final String CONFIGURATION_CATALOG_API_LOCATION = "org.opendatanode.CKAN.api.url";

    public static final String CONFIGURATION_HTTP_HEADER = "org.opendatanode.CKAN.http.header.";

    private static final String GENERATED_TABLE_NAME = "GENERATED_DATA";

    public static final int REQUEST_RECORD_LIMIT = 1000;

    public static final String[] columnNames = { "_id", "id", "data", "modified_timestamp", "deleted_timestamp" };

    private Set<String> datastoreResourceIds;

    private List<Dataset> publicDatasets;

    @DataUnit.AsOutput(name = "output")
    public WritableRelationalDataUnit output;

    @ExtensionInitializer.Init
    public FaultTolerance faultTolerance;

    private DPUContext context;

    private List<GenTableRow> inputFromCkan;

    public GeneratedToRelational() {
        super(GeneratedToRelationalVaadinDialog.class, ConfigHistory.noHistory(GeneratedToRelationalConfig_V1.class));
    }

    @Override
    protected void innerExecute() throws DPUException {
        Connection conn = null;
        try {
            createOutputTable();
            fillInputTableFromCKAN();
            LOG.debug("{} records retrieved from CKAN.", inputFromCkan.size());
            Collections.sort(inputFromCkan, new Comparator<GenTableRow>() {
                @Override
                public int compare(GenTableRow o1, GenTableRow o2) {
                    return Long.compare(o1.id, o2.id);
                }
            });
            this.output.addExistingDatabaseTable(GENERATED_TABLE_NAME.toUpperCase(), GENERATED_TABLE_NAME.toUpperCase());
            if (inputFromCkan.size() == 0) {
                LOG.debug("No input table found!");
                String dateToInsert = fmt.format(new Date());
                String insertIntoQuery = String.format("INSERT INTO %s VALUES (0, '%s');", GENERATED_TABLE_NAME, dateToInsert);
                LOG.debug("Inserting first data to Output table with query: " + insertIntoQuery);
                DatabaseHelper.executeUpdate(insertIntoQuery, output);
            } else {
                Integer todayInt = Integer.decode(intFmt.format(new Date()));
                PreparedStatement psInsert = null;
                PreparedStatement ps = null;
                int counter = 0;
                conn = output.getDatabaseConnection();
                for (GenTableRow currentRow : inputFromCkan) {
                    counter++;
//                    Integer lastRunInt = Integer.decode(intFmt.format(currentRow.modificationTimestamp));
                    Integer lastRunInt = Integer.decode(currentRow.data.substring(0, 10).replaceAll("-", "").trim());
                    LOG.debug(String.format("Last run date is: %d, today is: %d", lastRunInt, todayInt));
                    if (currentRow.deletedTimestamp != null) {
                        if (counter == inputFromCkan.size() && lastRunInt.compareTo(todayInt) < 0) {
                            LOG.debug("Starting to insert record.");
                            String dateToInsert = fmt.format(new Date());
                            String insertIntoQuery = String.format("INSERT INTO %s VALUES (%d,'%s');", GENERATED_TABLE_NAME, currentRow.id + 1, dateToInsert);
                            LOG.debug("Inserting data to Output table with query: " + insertIntoQuery);
                            DatabaseHelper.executeUpdate(insertIntoQuery, output);
                        }
                        LOG.debug("Commiting changes in output table.");
                        conn.commit();
                        continue;
                    } else if (lastRunInt.compareTo(todayInt) < 0) {
                        if (counter == inputFromCkan.size() && lastRunInt.compareTo(todayInt) < 0) {
                            LOG.debug("Starting to insert record.");
                            String dateToInsert = fmt.format(new Date());
                            String insertIntoQuery = String.format("INSERT INTO %s VALUES (%d,'%s');", GENERATED_TABLE_NAME, currentRow.id + 1, dateToInsert);
                            LOG.debug("Inserting data to Output table with query: " + insertIntoQuery);
                            DatabaseHelper.executeUpdate(insertIntoQuery, output);
                        }
                        LOG.debug("Commiting changes in output table.");
                        conn.commit();
                        continue;
                    }
                    String insertRecQuery = insertRecordForPrepStmt(GENERATED_TABLE_NAME, currentRow.id, currentRow.data);
                    LOG.debug("Executing query: {}", insertRecQuery);
                    psInsert = conn.prepareStatement(insertRecQuery);
                    psInsert.execute();
                    conn.commit();
//                    if (lastRunInt.compareTo(todayInt) < 0 && currentRow.deletedTimestamp == null) {
//                        LOG.debug("Starting to update and delete records.");
//                        String queryModify = updateModifyRecordForPrepStmt(GENERATED_TABLE_NAME.toUpperCase(), "data", fmt.format(new Date()));
//                        ps = conn.prepareStatement(queryModify);
//                        ps.setLong(1, currentRow.id);
//                        LOG.debug("Executing query: {} for ID: {}", queryModify, currentRow.id.toString());
//                        ps.execute();
//                        String queryDelete = deleteRecordForPrepStmt(GENERATED_TABLE_NAME.toUpperCase());
//                        ps = conn.prepareStatement(queryDelete);
//                        ps.setLong(1, currentRow.id);
//                        LOG.debug("Executing query: {} for ID: {}", queryDelete, currentRow.id.toString());
//                        ps.execute();
//                    } else if (lastRunInt.compareTo(todayInt) == 0 && currentRow.deletedTimestamp == null) {
                    if (lastRunInt.compareTo(todayInt) == 0 && currentRow.deletedTimestamp == null) {
                        LOG.debug("Starting to update records.");
                        String queryModify = updateModifyRecordForPrepStmt(GENERATED_TABLE_NAME.toUpperCase(), "data", fmt.format(new Date()));
                        ps = conn.prepareStatement(queryModify);
                        ps.setLong(1, currentRow.id);
                        LOG.debug("Executing query: {} for ID: {}", queryModify, currentRow.id.toString());
                        ps.execute();
                    }
                    LOG.debug("Commiting changes in output table.");
                    conn.commit();
                }
            }
            this.faultTolerance.execute(new FaultTolerance.Action() {

                @Override
                public void action() throws Exception {
                    eu.unifiedviews.helpers.dataunit.resource.Resource resource = ResourceHelpers.getResource(GeneratedToRelational.this.output, GENERATED_TABLE_NAME);
                    Date now = new Date();
                    resource.setCreated(now);
                    resource.setLast_modified(now);
                    ResourceHelpers.setResource(GeneratedToRelational.this.output, GENERATED_TABLE_NAME, resource);
                }
            });

        } catch (DataUnitException | SQLException ex) {
            ContextUtils.sendError(this.ctx, "errors.dpu.failed", ex, "errors.dpu.failed");
            return;
        } finally {
            DatabaseHelper.tryCloseConnection(conn);
        }

    }

    private List<ColumnDefinition> createColumnDefinitions() {
        List<ColumnDefinition> resultist = new ArrayList<ColumnDefinition>();
        resultist.add(new ColumnDefinition("id", "int8", 0, true, "Long"));
        resultist.add(new ColumnDefinition("data", "varchar", 0, true, "String"));
        return resultist;
    }

    private void createOutputTable() {
        List<ColumnDefinition> tableColumns = createColumnDefinitions();
        String createTableQuery = null;
        try {
            createTableQuery = getCreateTableQueryFromMetaData(tableColumns, GENERATED_TABLE_NAME);
            LOG.debug("Creating output table with query: " + createTableQuery);
            DatabaseHelper.executeUpdate(createTableQuery, GeneratedToRelational.this.output);
            List<String> primaryKey = new ArrayList<String>();
            primaryKey.add("\"id\"");
            String setNotNullQuery = createAlterColumnSetNotNullQuery(GENERATED_TABLE_NAME, "\"id\"");
            LOG.debug("Setting NOT NULL on ID column with query: " + setNotNullQuery);
            DatabaseHelper.executeUpdate(setNotNullQuery, GeneratedToRelational.this.output);
            String primaryKeysQuery = createPrimaryKeysQuery(GENERATED_TABLE_NAME, primaryKey);
            LOG.debug("Setting ID column as primary key with query: " + primaryKeysQuery);
            DatabaseHelper.executeUpdate(primaryKeysQuery, GeneratedToRelational.this.output);
        } catch (Exception ex) {
            ContextUtils.sendError(this.ctx, "errors.dpu.failed", ex, "errors.create.outTable");
        }
    }

    private String updateModifyRecordForPrepStmt(String tableName, String columnName, String columnValue) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(tableName);
        sb.append(" SET ");
        sb.append("\"").append(columnName).append("\"");
        sb.append("='");
        sb.append(columnValue);
        sb.append("' where \"id\"=?");
        return sb.toString();
    }

    private String insertRecordForPrepStmt(String tableName, Long id, String data) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(tableName);
        sb.append(" (\"id\", \"data\") ");
        sb.append("VALUES (");
        sb.append(id.toString()).append(", ");
        sb.append("'").append(data).append("'");
        sb.append(")");
        return sb.toString();
    }

    private class GenTableRow {
        Long id;

        String data;

        Date modificationTimestamp;

        Date deletedTimestamp;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("id = ").append(id != null ? Long.toString(id) : "null");
            sb.append(", data = ").append(data != null ? data : "null");
            sb.append(", modified = ").append(modificationTimestamp != null ? fmt.format(modificationTimestamp) : "null");
            sb.append(", deleted = ").append(deletedTimestamp != null ? fmt.format(deletedTimestamp) : "null");
            return sb.toString();
        }
    }

    private void fillInputTableFromCKAN() throws DPUException {
        this.context = this.ctx.getExecMasterContext().getDpuContext();
        String shortMessage = this.ctx.tr("dpu.ckan.starting", this.getClass().getSimpleName());
        String longMessage = String.valueOf(this.config);
        this.context.sendMessage(DPUContext.MessageType.INFO, shortMessage, longMessage);

        Map<String, String> environment = this.context.getEnvironment();
        final long pipelineId = this.context.getPipelineId();
        final String userId = this.context.getPipelineExecutionOwnerExternalId();
        String token = environment.get(CONFIGURATION_SECRET_TOKEN);
        if (StringUtils.isEmpty(token)) {
            throw ContextUtils.dpuException(this.ctx, "errors.token.missing");
        }

        String catalogApiLocation = environment.get(CONFIGURATION_CATALOG_API_LOCATION);
        if (StringUtils.isEmpty(catalogApiLocation)) {
            throw ContextUtils.dpuException(this.ctx, "errors.api.missing");
        }

        Map<String, String> additionalHttpHeaders = new HashMap<>();
        for (Map.Entry<String, String> configEntry : environment.entrySet()) {
            if (configEntry.getKey().startsWith(CONFIGURATION_HTTP_HEADER)) {
                String headerName = configEntry.getKey().replace(CONFIGURATION_HTTP_HEADER, "");
                String headerValue = configEntry.getValue();
                additionalHttpHeaders.put(headerName, headerValue);
            }
        }

        CatalogApiConfig apiConfig = new CatalogApiConfig(catalogApiLocation, pipelineId, userId, token, additionalHttpHeaders);
        if (ctx.canceled()) {
            throw ContextUtils.dpuExceptionCancelled(ctx);
        }
        getDatastoreResourceIds(apiConfig);
        getPackages(apiConfig);
        String resourceId = null;
        for (Dataset dataset : publicDatasets) {

            List<Resource> datastoreResources = dataset.getDatastoreResources(datastoreResourceIds);
            if (datastoreResources.isEmpty()) {
                continue;
            }
            StringBuilder sbResources = new StringBuilder();
            for (Resource res : datastoreResources) {
                if (sbResources.length() != 0) {
                    sbResources.append(", ");
                }
                sbResources.append(res.getName());
                if (res.getName().equals(GENERATED_TABLE_NAME)) {
                    LOG.debug("Resource {} found in CKAN with ID: {}", GENERATED_TABLE_NAME, res.getId());
                    resourceId = res.getId();
                }
            }
            LOG.debug("Public resources found in CKAN: {}", sbResources.toString());
        }
        // get the data from CKAN in chunks
        try {
            boolean recordsRemain = true;
            int offset = 0;
            inputFromCkan = new ArrayList<GeneratedToRelational.GenTableRow>();
            while (!ctx.canceled() && recordsRemain) {
                LOG.debug("requesting record from {0} to {1}", offset, (offset + REQUEST_RECORD_LIMIT - 1));
                DatastoreSearchResult result = new RelationalFromCkanHelper(ctx).getDatastoreSearchResult(apiConfig, resourceId, REQUEST_RECORD_LIMIT, offset);
                for (Record r : result.records) {
                    String values = r.getSqlInsertValues(Arrays.asList(columnNames), false);
                    LOG.debug("Record retrieved from CKAN: {}", values);
                    inputFromCkan.add(parseValues(values));
                }
                recordsRemain = offset + REQUEST_RECORD_LIMIT < result.total;
                offset += result.records.size();
            }
        } catch (Exception e) {
            LOG.error("Failed to retrieve datastore resource data.");
        }

    }

    private void getDatastoreResourceIds(CatalogApiConfig apiConfig) {
        if (apiConfig == null) {
            LOG.warn("ApiConfig is null!");
            return;
        }

        try {
            datastoreResourceIds = new RelationalFromCkanHelper(ctx).getDatastoreResourceIds(apiConfig);
        } catch (Exception e) {
            LOG.warn("Error loading CKAN datastore resource Ids: " + e.getMessage());
            datastoreResourceIds = Collections.emptySet();
        }
    }

    private void getPackages(CatalogApiConfig apiConfig) {
        if (apiConfig == null) {
            LOG.warn("ApiConfig is null!");
            return;
        }

        try {
            publicDatasets = new RelationalFromCkanHelper(ctx).getPackageListWithResources(apiConfig);
        } catch (Exception e) {
            LOG.error("Failed to retrieve datasets from CKAN!", e);
            publicDatasets = null;
        }
    }

    private GenTableRow parseValues(String values) {
        GenTableRow gtr = new GenTableRow();
        values = values.replace("(", "");
        values = values.replace(")", "");
        values = values.replace("'", "");
        values = values.replace(" ", "");
        values = values.replace("T", " ");
        String[] fields = values.split(",");
        try {
            gtr.id = Long.decode(fields[1]);
        } catch (NumberFormatException ex) {
            LOG.error("Error parsing ID from response from CKAN.");
        }
        try {
            gtr.data = fields[2];
            gtr.modificationTimestamp = fmt.parse(fields[3]);
            if (!fields[4].equalsIgnoreCase("null")) {
                gtr.deletedTimestamp = fmt.parse(fields[4]);
            } else {
                gtr.deletedTimestamp = null;
            }
        } catch (ParseException ex) {
            LOG.error("Error parsing dates from response from CKAN.");
        }
        LOG.debug("Parsed data: {}", gtr.toString());
        return gtr;
    }

    private String getCreateTableQueryFromMetaData(List<ColumnDefinition> columns, String tableName) throws SQLException {
        StringBuilder query = new StringBuilder();
        query.append("CREATE TABLE ");
        query.append(tableName);
        query.append(" (");
        for (ColumnDefinition column : columns) {
            query.append("\"");
            query.append(column.getColumnName());
            query.append("\"");
            query.append(" ");
            query.append(column.getColumnTypeName());
            if (column.getColumnSize() != -1) {
                query.append("(");
                query.append(column.getColumnSize());
                query.append(")");
            }
            if (column.isNotNull()) {
                query.append(" ");
                query.append("NOT NULL");
            }
            query.append(", ");
        }

        query.setLength(query.length() - 2);
        query.append(")");

        return query.toString();
    }

    private String createAlterColumnSetNotNullQuery(String tableName, String keyColumn) {
        StringBuilder query = new StringBuilder();
        query.append("ALTER TABLE ");
        query.append(tableName);
        query.append(" ");
        query.append("ALTER COLUMN ");
        query.append(keyColumn);
        query.append(" ");
        query.append("SET NOT NULL");

        return query.toString();
    }

    private String createPrimaryKeysQuery(String tableName, List<String> primaryKeys) {
        StringBuilder query = new StringBuilder("ALTER TABLE ");
        query.append(tableName);
        query.append(" ADD PRIMARY KEY (");
        for (String key : primaryKeys) {
            query.append(key);
            query.append(",");
        }
        query.setLength(query.length() - 1);
        query.append(")");

        return query.toString();
    }

    public class ColumnDefinition {

        private String columnName;

        private String columnTypeName;

        private int columnType;

        private boolean columnNotNull;

        private int columnSize;

        private String typeClassName;

        public ColumnDefinition(String columnName, String columnTypeName, int columnType, boolean columnNotNull, String typeClass, int columnSize) {
            this.columnName = columnName;
            this.columnTypeName = columnTypeName;
            this.columnType = columnType;
            this.columnNotNull = columnNotNull;
            this.columnSize = columnSize;
            this.typeClassName = typeClass;
        }

        public ColumnDefinition(String columnName, String columnTypeName, int columnType, boolean columnNotNull, String typeClass) {
            this(columnName, columnTypeName, columnType, columnNotNull, typeClass, -1);
        }

        public String getColumnName() {
            return this.columnName;
        }

        public String getColumnTypeName() {
            return this.columnTypeName;
        }

        public int getColumnType() {
            return this.columnType;
        }

        public boolean isNotNull() {
            return this.columnNotNull;
        }

        public int getColumnSize() {
            return this.columnSize;
        }

        public String getTypeClassName() {
            return this.typeClassName;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ColumnDefinition)) {
                return false;
            }
            ColumnDefinition cd = (ColumnDefinition) o;
            if (this.columnName.equals(cd.getColumnName()) && this.columnType == cd.getColumnType()) {
                return true;
            } else {
                return false;
            }
        }
    }

}
