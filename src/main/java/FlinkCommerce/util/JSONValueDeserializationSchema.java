package FlinkCommerce.util;

import FlinkCommerce.dto.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.sql.Date;

public class JSONValueDeserializationSchema implements DeserializationSchema<Transaction> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public Transaction deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Transaction.class);
    }

    @Override
    public boolean isEndOfStream(Transaction transaction) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }

    @Data
    @AllArgsConstructor
    public static class SalesPerMonth {
        private int year;
        private int month;
        private double totalSales;
    }

    @Data
    @AllArgsConstructor
    public static class SalesPerDay {
        private Date transactionDate;
        private Double totalSales ;
    }
}
