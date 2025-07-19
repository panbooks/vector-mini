use std::{
    num::{NonZeroU16, NonZeroU64},
    path::PathBuf,
};


impl Variant {
    pub async fn create_sender_receiver<T>(&self) -> (BufferSender<T>, BufferReceiver<T>)
    where
        T: Bufferable + Clone + Finalizable,
    {
        let mut builder = TopologyBuilder::default();
        match self {
            Variant::Memory {
                size, when_full, ..
            } => {
                builder.stage(MemoryBuffer::new(*size), *when_full);
            }
            Variant::DiskV2 {
                max_size,
                when_full,
                data_dir,
                id,
            } => {
                builder.stage(
                    DiskV2Buffer::new(id.clone(), data_dir.clone(), *max_size),
                    *when_full,
                );
            }
        }

        let (sender, receiver) = builder
            .build(String::from("benches"), Span::none())
            .await
            .unwrap_or_else(|_| unreachable!("topology build should not fail"));

        (sender, receiver)
    }
}



