package com.bigdataboutique.khose.config;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.shaded.com.google.common.base.Objects;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class KhoseConfigManager {

	public static final String DEFAULT_HOSE_NAME = "default";

	private static KhoseConfigManager Instance = null;

	private static final Log LOG = LogFactory.getLog(KhoseConfigManager.class);
	private final Object config;

	public static void initialize(final String fileName) {
		if (Instance == null) {
			synchronized (KhoseConfigManager.class) {
				if (Instance == null) {
					try {
						Instance = new KhoseConfigManager(fileName);
					} catch (IOException e) {
						LOG.error("Error while initializing configurations", e);
					}
				}
			}
		}
	}

	public static String get(final String property) {
		return get(DEFAULT_HOSE_NAME, property);
	}

	public static String getOrDefault(final String property, final String d) {
		return getOrDefault(DEFAULT_HOSE_NAME, property, d);
	}

	public static String getOrDefault(final String hoseName, final String property, final String d) {
		return Objects.firstNonNull(get(hoseName, property), d);
	}

	public static String get(final String hoseName, final String property) {
		try {
			return (String) PropertyUtils.getNestedProperty(Instance.config,
					"hoses." + hoseName + "." + property);
		} catch (org.apache.commons.beanutils.NestedNullException e) {
			throw new RuntimeException("Configuration hoses." + hoseName + "." + property + " does not exist");
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			LOG.error("Error getting config hoses." + hoseName + "." + property, e);
		}
		return null;
	}

	private KhoseConfigManager(String fileName) throws IOException {
		final Yaml yaml = new Yaml();
		this.config = yaml.load(FileUtils.readFileToString(new File(fileName), "UTF-8"));
	}
}