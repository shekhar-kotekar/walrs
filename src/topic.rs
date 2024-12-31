use uuid::Uuid;

use crate::partition::Partition;

pub struct Topic {
    id: Uuid,
    name: String,
    partitions: Vec<Partition>,
    retention_period: u16,
}

impl Topic {
    pub fn new(name: String, num_partitions: u8, retention_period: u16) -> Topic {
        let partitions = Topic::create_partitions(&name, num_partitions, retention_period);
        Topic {
            id: Uuid::new_v4(),
            name,
            partitions,
            retention_period: retention_period,
        }
    }

    fn create_partitions(
        topic_name: &str,
        num_partitions: u8,
        retention_period: u16,
    ) -> Vec<Partition> {
        let mut partitions = Vec::new();
        for i in 0..num_partitions {
            partitions.push(Partition::new(i, topic_name.to_string(), retention_period));
        }
        partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_topic() {
        let num_partitions = 3;
        let retention_period_days = 7;
        let topic = Topic::new(
            "test_topic".to_string(),
            num_partitions,
            retention_period_days,
        );
        assert_eq!(topic.name, "test_topic");
        assert_eq!(topic.partitions.len(), 3);
        assert_eq!(topic.retention_period, 7);
    }
}
