package com.example;
import java.nio.charset.StandardCharsets;

public class ShardCalculator {

    public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc); // round down to 4 byte block

        for (int i = offset; i < roundedEnd; i += 4) {
            int k1 = ((data[i] & 0xff)) |
                    ((data[i + 1] & 0xff) << 8) |
                    ((data[i + 2] & 0xff) << 16) |
                    (data[i + 3] << 24);

            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        int k1 = 0;
        int tail = len & 0x03;
        if (tail == 3) k1 ^= (data[roundedEnd + 2] & 0xff) << 16;
        if (tail >= 2) k1 ^= (data[roundedEnd + 1] & 0xff) << 8;
        if (tail >= 1) {
            k1 ^= (data[roundedEnd] & 0xff);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
        }

        h1 ^= len;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);

        return h1;
    }

    public static int computeShard(String id, int numPrimaryShards) {
        byte[] bytes = id.getBytes(StandardCharsets.UTF_8);
        int hash = murmurhash3_x86_32(bytes, 0, bytes.length, 0);
        return (hash & 0x7fffffff) % numPrimaryShards;
    }

    public static void main(String[] args) {
        String id = "2";
        int shards = 10;
        System.out.println("Shard = " + computeShard(id, shards));
    }
}