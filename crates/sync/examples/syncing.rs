use std::{
    path::PathBuf,
    sync::Arc,
    thread::{self, JoinHandle, Thread},
    time::Duration,
};

use chunk::HashChunks;
use crdts::CmRDT;
use libp2p::PeerId;
use parking_lot::Mutex;
use sync::SyncingDrive;
use utils::{tree_stringify, workspace};
use vindex::VIndex;

pub struct SyncingApp {
    drive: Arc<SyncingDrive>,
    drive2: Arc<SyncingDrive>,
    vindex: Arc<Mutex<VIndex<HashChunks>>>,
    thread: JoinHandle<()>,
}

impl SyncingApp {
    pub fn new() -> Self {
        let drive = Arc::new(SyncingDrive::new(
            PathBuf::from(workspace!("test_dir")),
            PathBuf::from(workspace!("cache")),
            PeerId::random(),
        ));

        let drive2 = Arc::new(SyncingDrive::new(
            PathBuf::from(workspace!("test_dir2")),
            PathBuf::from(workspace!("cache2")),
            PeerId::random(),
        ));

        let vindex = Arc::new(Mutex::new(VIndex::default()));

        let vindex_for_thread = vindex.clone();
        let drive2_for_thread = drive2.clone();
        let drive_for_thread = drive.clone();
        let thread = std::thread::spawn(move || loop {
            let mut vindex = vindex_for_thread.lock();
            let other_ops = drive_for_thread.vindex.lock().ops_after(&vindex.clock());

            for op in other_ops {
                vindex.apply(op);
            }

            let other_ops = drive2_for_thread.vindex.lock().ops_after(&vindex.clock());

            for op in other_ops {
                vindex.apply(op);
            }

            std::mem::drop(vindex);

            std::thread::sleep(Duration::from_secs(1))
        });

        Self {
            drive,
            drive2,
            vindex,
            thread,
        }
    }
}

impl eframe::App for SyncingApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::Window::new("drive1").show(&ctx, |ui| self.drive.debug_ui(ui));
        egui::Window::new("drive2").show(&ctx, |ui| self.drive2.debug_ui(ui));
        egui::Window::new("merged").show(&ctx, |ui| {
            ui.text_edit_multiline(&mut tree_stringify(self.vindex.lock().iter(), "/"));
        });
    }
}

fn main() -> eframe::Result<()> {
    tracing_subscriber::fmt::init();

    std::fs::remove_dir_all(workspace!("cache"));
    std::fs::remove_dir_all(workspace!("cache2"));

    let native_options = eframe::NativeOptions::default();
    eframe::run_native(
        "Atomic Drive [Debug UI]",
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
