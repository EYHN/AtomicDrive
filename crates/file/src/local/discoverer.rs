use super::{Configuration, Result, Walker, WalkerItem};

pub struct Discoverer {
    configuration: Configuration,
    current_walker: Option<Walker>,
}

impl Discoverer {
    pub fn new(configuration: Configuration) -> Self {
        Self {
            configuration: configuration.clone(),
            current_walker: None,
        }
    }

    // pub fn poll_ops(&mut self) -> Result<()> {
    //     if let WalkerItem::Reached {
    //         folder,
    //         metadata: _,
    //         children,
    //     } = self.poll_walker()?
    //     {
    //         let mut transaction = self.tracker.start_transaction()?;
    //         let ops = transaction.apply(Discovery {
    //             entities: children
    //                 .into_iter()
    //                 .map(|(name, metadata)| DiscoveryEntity {
    //                     name: self.convert_name(&name),
    //                     marker: self.make_marker(&metadata),
    //                     type_marker: self.make_type_marker(&metadata),
    //                     update_marker: self.make_update_marker(&metadata),
    //                 })
    //                 .collect(),
    //             location: (self.convert_path(&folder).unwrap(), Default::default()),
    //         })?;
    //         transaction.commit()?;
    //         if !ops.is_empty() {
    //             dbg!(ops);
    //         }
    //     }

    //     Ok(())
    // }

    fn poll_changes(&mut self) -> Result<WalkerItem> {
        self.poll_walker()
    }

    fn poll_walker(&mut self) -> Result<WalkerItem> {
        let walker = if let Some(ref mut walker) = &mut self.current_walker {
            walker
        } else {
            self.current_walker = Some(Walker::new(&self.configuration.root));
            self.current_walker.as_mut().unwrap()
        };

        if let Some(next) = walker.iter().next() {
            Ok(next?)
        } else {
            Ok(WalkerItem::Pending)
        }
    }
}
