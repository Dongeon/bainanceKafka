package org.example.repository;

import org.example.model.ProducerStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class ProducerStatusRepository {

    private final JdbcTemplate jdbc;

    public ProducerStatusRepository(@Qualifier("isetdxJdbc") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<ProducerStatus> findAll() {
        String sql = "SELECT instance_id, weight, state, received_at, updated_at " +
                     "FROM producer_heartbeat_realtime ORDER BY instance_id ASC";
        return jdbc.query(sql, new RowMapper<ProducerStatus>() {
            @Override
            public ProducerStatus mapRow(ResultSet rs, int rowNum) throws SQLException {
                return new ProducerStatus(
                    rs.getString("instance_id"),
                    rs.getInt("weight"),
                    rs.getString("state"),
                    rs.getTimestamp("received_at").toLocalDateTime(),
                    rs.getTimestamp("updated_at").toLocalDateTime()
                );
            }
        });
    }
}
