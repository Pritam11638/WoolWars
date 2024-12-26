package org.pritam.HikariCP.records;

public record SQL(String query, Object[] params) {}