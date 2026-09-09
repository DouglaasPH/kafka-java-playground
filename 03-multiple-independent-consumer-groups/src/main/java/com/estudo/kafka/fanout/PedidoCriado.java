package com.estudo.kafka.fanout;

import java.math.BigDecimal;

public record PedidoCriado(String pedidoId, String cliente, BigDecimal total) {
    public String toJson() {
        return """
                {"pedidoId":"%s","cliente":"%s","total":%s}""".formatted(
                        pedidoId, cliente, total.toPlainString());
    }
}
