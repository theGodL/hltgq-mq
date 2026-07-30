package com.qgyun.hltgq.hltgqmq.util;

import java.security.SecureRandom;

/**
 * 统一ID生成器 —— 20位短ID，与站点表规则一致
 */
public final class IdGenerator {

    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int LENGTH = 20;
    private static final SecureRandom RANDOM = new SecureRandom();

    private IdGenerator() {}

    public static String generate() {
        char[] chars = new char[LENGTH];
        for (int i = 0; i < LENGTH; i++) {
            chars[i] = CHARS.charAt(RANDOM.nextInt(CHARS.length()));
        }
        return new String(chars);
    }
}
