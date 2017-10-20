/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.support;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageUtil {

	private static final Logger log = LoggerFactory.getLogger(PackageUtil.class);

	public static final String JAR_URL_SEPARATOR = "!/";

	private static void doFetchInPath(Set<Class<?>> classes, File directory, String packageName,
			ClassLoader classLoader) throws ClassNotFoundException {
		File[] dirContents = directory.listFiles();
		if (dirContents == null) {
			throw new ClassNotFoundException("invalid directory " + directory.getAbsolutePath());
		}
		for (File file : dirContents) {
			String fileName = file.getName();
			if (file.isDirectory()) {
				doFetchInPath(classes, file, packageName + "." + fileName, classLoader);
			} else if (fileName.endsWith(".class")) {
				classes.add(classLoader.loadClass(packageName + '.' + fileName.substring(0, fileName.length() - 6)));
			}
		}
	}

	public static Set<Class<?>> getClasses(String packagePath) throws ClassNotFoundException, IOException {
		Set<Class<?>> classes = new HashSet<Class<?>>();
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		if (classLoader == null) {
			throw new ClassNotFoundException("class loader not found for current thread");
		}
		Enumeration<URL> resources = null;
		try {
			resources = classLoader.getResources(packagePath.replace('.', '/'));
		} catch (IOException e) {
			throw new ClassNotFoundException("invalid package " + packagePath, e);
		}
		while (resources.hasMoreElements()) {
			URL url = resources.nextElement();
			if (url == null) {
				throw new ClassNotFoundException(packagePath + " - package not found");
			}
			String dirPath = fastReplace(url.getFile(), "%20", " ");
			int jarSeparator = dirPath.indexOf(JAR_URL_SEPARATOR);
			if (jarSeparator == -1) {
				File directory = new File(dirPath);
				if (!directory.exists()) {
					throw new ClassNotFoundException(packagePath + " - invalid package");
				}
				doFetchInPath(classes, directory, packagePath, classLoader);
			} else {
				String rootEntry = dirPath.substring(jarSeparator + JAR_URL_SEPARATOR.length());
				if (!"".equals(rootEntry) && !rootEntry.endsWith("/")) {
					rootEntry = rootEntry + "/";
				}
				JarFile jarFile = null;
				try {
					URLConnection con = url.openConnection();
					if (con instanceof JarURLConnection) {
						JarURLConnection jarCon = (JarURLConnection) con;
						jarCon.setUseCaches(false);
						jarFile = jarCon.getJarFile();
					} else {
						String jarName = dirPath.substring(0, jarSeparator);
						jarName = fastReplace(jarName, " ", "%20");
						jarFile = new JarFile(jarName);
					}
					for (Enumeration<JarEntry> entries = jarFile.entries(); entries.hasMoreElements();) {
						JarEntry entry = entries.nextElement();
						String fileName = entry.getName();
						if (fileName.startsWith(rootEntry) && fileName.endsWith(".class")) {
							fileName = fileName.replace('/', '.');
							try {
								classes.add(classLoader.loadClass(fileName.substring(0, fileName.length() - 6)));
							} catch (ClassNotFoundException e) {
								log.error("class load fail", e);
							}
						}
					}
				} catch (IOException e) {
					throw new ClassNotFoundException("jar fail", e);
				} finally {
					if (jarFile != null)
						jarFile.close();
				}
			}
		}
		return classes;
	}

	public static String fastReplace(String inString, String oldPattern, String newPattern) {
		if (inString == null) {
			return null;
		}
		if (oldPattern == null || newPattern == null) {
			return inString;
		}
		StringBuilder sbuf = new StringBuilder();
		int pos = 0;
		int index = inString.indexOf(oldPattern);
		int patLen = oldPattern.length();
		while (index >= 0) {
			sbuf.append(inString.substring(pos, index));
			sbuf.append(newPattern);
			pos = index + patLen;
			index = inString.indexOf(oldPattern, pos);
		}
		sbuf.append(inString.substring(pos));
		return sbuf.toString();
	}
}
