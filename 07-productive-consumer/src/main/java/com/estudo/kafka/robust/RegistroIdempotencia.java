package com.estudo.kafka.robust;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RegistroIdempotencia {
    // Store em memomia (didatico). Em producao: Redis, banco relacional
    // com constraint UNIQUE, etc. A chave usada aqui e "particao-offset",
    //que e naturalmente unica por registro dentro de um topico.
    private static final Set<String> PROCESSADOS = ConcurrentHashMap.newKeySet();

    public static boolean jaProcessado(int particao, long offset) {
        return PROCESSADOS.contains(chave(particao, offset));
    }

    public static void marcarComoProcessado(int particao, long offset) {
        PROCESSADOS.add(chave(particao, offset));
    }

    public static String chave(int particao, long offset) {
        return particao + "-" + offset;
    }
}
