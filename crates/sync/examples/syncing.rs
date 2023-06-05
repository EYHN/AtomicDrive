use std::path::PathBuf;

use chunk::{chunks, HashChunks};
use crdts::CmRDT;
use file::FileType;
use file_local::{LocalFileSystem, LocalFileSystemConfiguration};
use libp2p::PeerId;
use utils::tree_stringify;
use vindex::{IndexPeerId, VIndex};

pub struct SyncingApp {
    file_system: LocalFileSystem,
    device: VIndex<HashChunks>,
}

impl SyncingApp {
    pub fn new() -> Self {
        let fs = LocalFileSystem::new(LocalFileSystemConfiguration {
            root: PathBuf::from("/Users/eyhn/Projects/AtomicDrive/test_dir"),
            data_dir: PathBuf::from("/Users/eyhn/Projects/AtomicDrive/cache"),
        });

        let device_peer_id = IndexPeerId::from(PeerId::random());

        let mut device = VIndex::default();

        for event in fs.quick_full_walk() {
            match event.event_type {
                file::FileEventType::Created | file::FileEventType::Changed => {
                    let stat = fs.stat_file(event.path.clone());
                    if stat.file_type == FileType::File {
                        let data = fs.read_file(event.path.clone());
                        device.apply(device.write(
                            event.path.to_string(),
                            chunks(&data),
                            stat.last_write_time,
                            device_peer_id,
                            device.read_ctx().derive_add_ctx(device_peer_id),
                        ))
                    }
                }
                file::FileEventType::Deleted => device.apply(device.rm(
                    event.path.to_string(),
                    device.read_ctx().derive_add_ctx(device_peer_id),
                )),
            }
        }

        Self {
            file_system: fs,
            device,
        }
    }
}

impl eframe::App for SyncingApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(&ctx, |ui| {
            ui.add(egui::Label::new("Hello World!"));
            ui.label("A shorter and more convenient way to add a label.");
            ui.text_edit_multiline(&mut tree_stringify(self.device.iter(), "/"));
            if ui.button("Click me").clicked() {
                // take some action here
            }
        });
    }
}

fn main() -> eframe::Result<()> {
    tracing_subscriber::fmt::init();

    let native_options = eframe::NativeOptions::default();
    eframe::run_native(
        "Atomic Drive",
        native_options,
        Box::new(|cc| {
            let mut fonts = egui::FontDefinitions::default();

            fonts.font_data.insert(
                "FiraCode".to_owned(),
                egui::FontData::from_static(include_bytes!("FiraCode-Regular.ttf")),
            );

            fonts
                .families
                .entry(egui::FontFamily::Proportional)
                .or_default()
                .insert(0, "FiraCode".to_owned());

            cc.egui_ctx.set_fonts(fonts);
            Box::new(SyncingApp::new())
        }),
    )
}
