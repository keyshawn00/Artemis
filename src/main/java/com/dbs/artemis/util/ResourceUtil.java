package com.dbs.artemis.util;

import lombok.experimental.UtilityClass;

import java.io.InputStream;

@UtilityClass
public class ResourceUtil {

    public static InputStream getResource(String path) {
        return ResourceUtil.class.getClassLoader().getResourceAsStream(path);
    }
}
