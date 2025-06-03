package com.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.InetSocketAddress;

public class App {
    public static void main(String[] args) {
        String csvFile = "src/main/java/com/example/policy.csv";
        int batchSize = 100;

        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("172.17.0.2", 9042))
                .withLocalDatacenter("datacenter1")
                .build();
             BufferedReader br = new BufferedReader(new FileReader(csvFile));
             CSVParser parser = new CSVParser(br, CSVFormat.DEFAULT
                     .withDelimiter(';')
                     .withQuote('"')
                     .withFirstRecordAsHeader())) {

            String insertQuery = "INSERT INTO climate_policies.policies " +
                    "(\"Index\", Policy_raw, Year, IPCC_Region, Policy_Type, sector, bm25_score_first) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement prepared = session.prepare(insertQuery);

            BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
            int currentBatchSize = 0;

            for (CSVRecord record : parser) {
                Integer index = parseInt(record.get("Index"));
                String policyRaw = record.get("Policy_raw");
                Integer year = parseInt(record.get("Year"));
                String ipccRegion = record.get("IPCC_Region");
                String policyType = record.get("Policy_Type");
                String sector = record.get("sector");
                Float bm25ScoreFirst = parseFloat(record.get("bm25_score_first"));

                if (index == null || year == null || ipccRegion == null || ipccRegion.isEmpty()) {
                    System.out.println("Skipping row with missing primary key field: " + record.toString());
                    continue;
                }

                batchBuilder.addStatement(prepared.bind(
                        index,
                        policyRaw,
                        year,
                        ipccRegion,
                        policyType,
                        sector,
                        bm25ScoreFirst
                ));

                currentBatchSize++;

                if (currentBatchSize >= batchSize) {
                    session.execute(batchBuilder.build());
                    batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
                    currentBatchSize = 0;
                }
            }

            if (currentBatchSize > 0) {
                session.execute(batchBuilder.build());
            }

            System.out.println("Importation terminée !");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Integer parseInt(String value) {
        if (value == null || value.isEmpty()) return null;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Float parseFloat(String value) {
        if (value == null || value.isEmpty()) return null;
        try {
            return Float.parseFloat(value.replace(",", "."));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
