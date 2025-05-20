package ru.crpt.ismp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class CrptApi {

    private static final Logger logger = Logger.getLogger(CrptApi.class.getName());
    private static final ObjectMapper objMapper = new ObjectMapper();
    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/commissioning/contract/create";
    private final String TOKEN = "token";

    private final Semaphore semaphore;
    private final ScheduledExecutorService executor;
    private final int requestLimit;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (requestLimit <= 0) {
            logger.severe("Количество запросов должен быть положительным: " + requestLimit);
            throw new IllegalArgumentException("Количество запросов(requestLimit) должен быть положительным");
        }
        this.requestLimit = requestLimit;
        this.semaphore = new Semaphore(requestLimit);
        this.executor = new ScheduledThreadPoolExecutor(1);

        startRateLimitTask(timeUnit);
    }

    /**
     * Восстановления разрешений семафора.
     * @param timeUnit Единица времени для интервала ограничения запросов.
     */
    private void startRateLimitTask(TimeUnit timeUnit) {
        long periodNanos = timeUnit.toNanos(1) / requestLimit;

        executor.scheduleAtFixedRate(() -> {
            if (semaphore.availablePermits() < requestLimit) {
                semaphore.release();
            }
        }, periodNanos, periodNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Создаёт документ для ввода товара в оборот через API "Честного знака".
     *
     * @param document Объект документа.
     * @param signature подпись УКЭП в Base64.
     * @return UUID созданного документа.
     */
    public String createDocument(Document document, String signature) {
        String docUuid = "";
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            semaphore.acquire();

            String encodedDoc = dataEncoder(document);
            String encodedSignature = dataEncoder(signature);

            RequestBody requestBody = new RequestBody(
                    RequestBody.DocumentFormat.MANUAL.name(),
                    encodedDoc,
                    ProductGroup.milk.name(),
                    encodedSignature,
                    document.docType
            );

            String json = objMapper.writeValueAsString(requestBody);

            HttpPost request = new HttpPost(API_URL);
            request.setHeader("Content-Type", "application/json");
            request.setHeader("Authorization", "Bearer " + TOKEN);
            request.setEntity(new StringEntity(json, "UTF-8"));

            try (CloseableHttpResponse response = client.execute(request)) {
                String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");
                Response responseObj = objMapper.readValue(responseBody, Response.class);

                if (Objects.nonNull(responseObj.getValue())) {
                    docUuid = responseObj.getValue();
                    logger.info("Создан документ с UUID: " + docUuid);
                } else {
                    logger.severe("Ошибка при создании документа: " + responseObj.getErrorMessage());
                    throw new RuntimeException("Ошибка при создании документа: " + responseObj.getErrorMessage());
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            logger.severe("Запрос был прерван");
            throw new RuntimeException("Запрос был прерван", ex);
        } catch (IOException ex) {
            logger.severe("Ошибка при выполнении запроса: " + ex.getMessage());
            throw new RuntimeException("Ошибка при выполнении запроса", ex);
        } finally {
            semaphore.release();
        }
        return docUuid;
    }

    private static Product productDocBase64Decoder(String productDocument) {
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(productDocument);
            return objMapper.readValue(decodedBytes, Product.class);
        } catch (IOException e) {
            logger.severe("Ошибка при парсинге productDocument: " + e.getMessage() + e);
            throw new RuntimeException("Ошибка при парсинге productDocument", e);
        }
    }

    private static String dataEncoder(Object data) {
        try {
            byte[] bytes = objMapper.writeValueAsBytes(data);
            return Base64.getEncoder().encodeToString(bytes);
        } catch (JsonProcessingException e) {
            logger.severe("Ошибка при парсинге: " + e.getMessage() + e);
            throw new RuntimeException("Ошибка при парсинге", e);
        }
    }

    public static class RequestBody {
        @JsonProperty("document_format")
        private final String documentFormat;
        @JsonProperty("product_document")
        private final String productDocument;
        @JsonProperty("product_group")
        private final String productGroup;
        @JsonProperty("signature")
        private final String signature;
        @JsonProperty("type")
        private final String type;

        public RequestBody(
                @JsonProperty("document_format") String documentFormat,
                @JsonProperty("product_document") String productDocument,
                @JsonProperty("product_group") String productGroup,
                @JsonProperty("signature") String signature,
                @JsonProperty("type") String type) {
            this.documentFormat = documentFormat;
            this.productDocument = productDocument;
            this.productGroup = productGroup;
            this.signature = signature;
            this.type = type;
        }

        public enum DocumentFormat {
            MANUAL,
            XML,
            CSV
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;

            RequestBody requestBody = (RequestBody) o;
            return documentFormat.equals(requestBody.documentFormat) && productDocument.equals(requestBody.productDocument) && Objects.equals(productGroup, requestBody.productGroup) && signature.equals(requestBody.signature) && type.equals(requestBody.type);
        }

        @Override
        public int hashCode() {
            int result = documentFormat.hashCode();
            result = 31 * result + productDocument.hashCode();
            result = 31 * result + Objects.hashCode(productGroup);
            result = 31 * result + signature.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }
    }

    public static class Response {
        @JsonProperty("value")
        private String value;
        @JsonProperty("error_message")
        private String errorMessage;

        public String getValue() {
            return value;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    public enum ProductGroup {
        clothes,
        shoes,
        tobacco,
        perfumery,
        tires,
        electronics,
        pharma,
        milk,
        bicycle,
        wheelchairs
    }

    public enum ProductionType {
        OWN_PRODUCTION,
        CONTRACT_PRODUCTION
    }

    public static class Product {
        @JsonProperty("certificate_document")
        private final String certificateDocument;

        @JsonProperty("certificate_document_date")
        private final String certificateDocumentDate;

        @JsonProperty("certificate_document_number")
        private final String certificateDocumentNumber;

        @JsonProperty("owner_inn")
        private final String ownerInn;

        @JsonProperty("producer_inn")
        private final String producerInn;

        @JsonProperty("production_date")
        private final String productionDate;

        @JsonProperty("tnved_code")
        private final String tnvedCode;

        @JsonProperty("uit_code")
        private final String uitCode;

        @JsonProperty("uitu_code")
        private String uituCode;

        @JsonCreator
        public Product(
                @JsonProperty("certificate_document") String certificateDocument,
                @JsonProperty("certificate_document_date") String certificateDocumentDate,
                @JsonProperty("certificate_document_number") String certificateDocumentNumber,
                @JsonProperty("owner_inn") String ownerInn,
                @JsonProperty("producer_inn") String producerInn,
                @JsonProperty("production_date") String productionDate,
                @JsonProperty("tnved_code") String tnvedCode,
                @JsonProperty("uit_code") String uitCode) {
            this.certificateDocument = certificateDocument;
            this.certificateDocumentDate = certificateDocumentDate;
            this.certificateDocumentNumber = certificateDocumentNumber;
            this.ownerInn = ownerInn;
            this.producerInn = producerInn;
            this.productionDate = productionDate;
            this.tnvedCode = tnvedCode;
            this.uitCode = uitCode;
        }

        public String getUituCode() {
            return uituCode;
        }

        public void setUituCode(String uituCode) {
            this.uituCode = uituCode;
        }

        public enum CertificateDocument {
            CONFORMITY_CERTIFICATE,
            CONFORMITY_DECLARATION
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;

            Product product = (Product) o;
            return Objects.equals(certificateDocument, product.certificateDocument) && Objects.equals(certificateDocumentDate, product.certificateDocumentDate) && Objects.equals(certificateDocumentNumber, product.certificateDocumentNumber) && ownerInn.equals(product.ownerInn) && producerInn.equals(product.producerInn) && productionDate.equals(product.productionDate) && tnvedCode.equals(product.tnvedCode) && Objects.equals(uitCode, product.uitCode) && Objects.equals(uituCode, product.uituCode);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(certificateDocument);
            result = 31 * result + Objects.hashCode(certificateDocumentDate);
            result = 31 * result + Objects.hashCode(certificateDocumentNumber);
            result = 31 * result + ownerInn.hashCode();
            result = 31 * result + producerInn.hashCode();
            result = 31 * result + productionDate.hashCode();
            result = 31 * result + tnvedCode.hashCode();
            result = 31 * result + Objects.hashCode(uitCode);
            result = 31 * result + Objects.hashCode(uituCode);
            return result;
        }

        @Override
        public String toString() {
            return "Product{" +
                   "certificateDocument='" + certificateDocument + '\'' +
                   ", certificateDocumentDate='" + certificateDocumentDate + '\'' +
                   ", certificateDocumentNumber='" + certificateDocumentNumber + '\'' +
                   ", ownerInn='" + ownerInn + '\'' +
                   ", producerInn='" + producerInn + '\'' +
                   ", productionDate='" + productionDate + '\'' +
                   ", tnvedCode='" + tnvedCode + '\'' +
                   ", uitCode='" + uitCode + '\'' +
                   ", uituCode='" + uituCode + '\'' +
                   '}';
        }
    }

    public static class Document {
        @JsonProperty("description")
        private final Description description;

        @JsonProperty("doc_id")
        private final String docId;

        @JsonProperty("doc_status")
        private String docStatus;

        @JsonProperty("doc_type")
        private final String docType;

        @JsonProperty("import_request")
        private String importRequest;

        @JsonProperty("owner_inn")
        private final String ownerInn;

        @JsonProperty("participant_inn")
        private final String participantInn;

        @JsonProperty("producer_inn")
        private final String producerInn;

        @JsonProperty("production_date")
        private final String productionDate;

        @JsonProperty("production_type")
        private final String productionType;

        @JsonProperty("products")
        private final List<Product> products;

        @JsonProperty("reg_date")
        private String regDate;

        @JsonProperty("reg_number")
        private String regNumber;

        @JsonCreator
        public Document(
                @JsonProperty("description") Description description,
                @JsonProperty("doc_id") String docId,
                @JsonProperty("doc_status") String docStatus,
                @JsonProperty("doc_type") String docType,
                @JsonProperty("owner_inn") String ownerInn,
                @JsonProperty("participant_inn") String participantInn,
                @JsonProperty("producer_inn") String producerInn,
                @JsonProperty("production_date") String productionDate,
                @JsonProperty("production_type") String productionType,
                @JsonProperty("products") List<Product> products,
                @JsonProperty("reg_date") String regDate
        ) {
            this.description = description;
            this.docId = docId;
            this.docStatus = docStatus;
            this.ownerInn = ownerInn;
            this.participantInn = participantInn;
            this.producerInn = producerInn;
            this.productionDate = productionDate;
            this.productionType = productionType;
            this.products = Optional.ofNullable(products).orElse(Collections.emptyList());
            this.regDate = regDate;
            this.docType = setDocType(docType);
        }

        public String getDocStatus() {
            return docStatus;
        }

        public void setDocStatus(String docStatus) {
            this.docStatus = docStatus;
        }

        public String getRegDate() {
            return regDate;
        }

        public void setRegDate(String regDate) {
            this.regDate = regDate;
        }

        public String getRegNumber() {
            return regNumber;
        }

        public void setRegNumber(String regNumber) {
            this.regNumber = regNumber;
        }

        private String setDocType(String docType) {
            String doc = docType.toUpperCase();
            return doc.contains("CSV") ? DocumentType.LP_INTRODUCE_GOODS_CSV.name() :
                    doc.contains("XML") ? DocumentType.LP_INTRODUCE_GOODS_XML.name() :
                            DocumentType.LP_INTRODUCE_GOODS.name();
        }

        public enum DocumentType {
            LP_INTRODUCE_GOODS,
            LP_INTRODUCE_GOODS_CSV,
            LP_INTRODUCE_GOODS_XML
        }

        public record Description(String participantInn) {
            @JsonCreator
            public Description {}

            @Override
            public boolean equals(Object o) {
                if (o == null || getClass() != o.getClass()) return false;

                Description desc = (Description) o;
                return participantInn.equals(desc.participantInn);
            }

            @Override
            public String toString() {
                return "Description{" +
                       "participantInn='" + participantInn + '\'' +
                       '}';
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;

            Document document = (Document) o;
            return Objects.equals(description, document.description) && docId.equals(document.docId) && docStatus.equals(document.docStatus) && docType.equals(document.docType) && Objects.equals(importRequest, document.importRequest) && ownerInn.equals(document.ownerInn) && participantInn.equals(document.participantInn) && producerInn.equals(document.producerInn) && productionDate.equals(document.productionDate) && productionType.equals(document.productionType) && Objects.equals(products, document.products) && regDate.equals(document.regDate) && Objects.equals(regNumber, document.regNumber);
        }

        @Override
        public int hashCode() {
            int result = docId.hashCode();
            result = 31 * result + Objects.hashCode(description);
            result = 31 * result + docStatus.hashCode();
            result = 31 * result + docType.hashCode();
            result = 31 * result + Objects.hashCode(importRequest);
            result = 31 * result + ownerInn.hashCode();
            result = 31 * result + participantInn.hashCode();
            result = 31 * result + producerInn.hashCode();
            result = 31 * result + productionDate.hashCode();
            result = 31 * result + productionType.hashCode();
            result = 31 * result + Objects.hashCode(products);
            result = 31 * result + regDate.hashCode();
            result = 31 * result + Objects.hashCode(regNumber);
            return result;
        }

        @Override
        public String toString() {
            return "Document{" +
                   "description=" + description +
                   ", docId='" + docId + '\'' +
                   ", docStatus='" + docStatus + '\'' +
                   ", docType='" + docType + '\'' +
                   ", importRequest='" + importRequest + '\'' +
                   ", ownerInn='" + ownerInn + '\'' +
                   ", participantInn='" + participantInn + '\'' +
                   ", producerInn='" + producerInn + '\'' +
                   ", productionDate='" + productionDate + '\'' +
                   ", productionType='" + productionType + '\'' +
                   ", products=" + products +
                   ", regDate='" + regDate + '\'' +
                   ", regNumber='" + regNumber + '\'' +
                   '}';
        }
    }

    private static String dateFormat(String date) {
        if (Objects.isNull(date)) {
            logger.severe("Дата не может быть null");
            throw new IllegalArgumentException("Дата не может быть null");
        }
        LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        return DateTimeFormatter
                .ofPattern("yyyy-MM-dd")
                .format(localDate.atStartOfDay(
                        ZoneId.of("Europe/Moscow"))
                );
    }

    private static String dateTimeNow() {
        return DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH:mm:ss")
                .withZone(ZoneId.of("Europe/Moscow"))
                .format(Instant.now());
    }
}
