//! The file seam (#213, decision 67): everything a volume does to one of its files. Production
//! uses `DiskFile` (and `DiskFs`), which does what the volume code does today; the
//! deterministic simulation uses `crate::sim::SimFile`, which can crash, tear and fail on a
//! seed.
//!
//! The volume code becomes generic over `VolFs` in stage 1b, with `DiskFs` as the default, so
//! production is monomorphised and pays nothing for the seam.

use crate::direct_io::AlignedFile;
use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Arc;

/// Which file of a volume is opened: the data file (direct I/O when `AWAN_DIRECT_IO=1`,
/// sector-aligned writes) or its journal (small appends through the page cache).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    Data,
    Journal,
    /// A metadata segment (#26): written once, then read with positioned reads.
    Segment,
}

/// One file of a volume.
pub trait VolFile: Send + Sync + 'static {
    /// A sector-aligned write at a sector-aligned offset (the data file's needles).
    fn write_aligned_at(&self, offset: u64, buf: &[u8]) -> io::Result<()>;
    /// A sector-aligned read at a sector-aligned offset, with direct I/O where it is on.
    fn read_aligned_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;
    /// A write of any length at any offset (the journal's records).
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()>;
    /// A read of any length at any offset through the page cache (GETs, #155).
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    /// The data and the length are durable when this returns.
    fn sync_data(&self) -> io::Result<()>;
    /// As `sync_data`, and the file's metadata too (used after recovery cuts a torn tail).
    fn sync_all(&self) -> io::Result<()> {
        self.sync_data()
    }
    /// Marks the file sparse where the platform needs it (Windows); a no-op elsewhere.
    fn make_sparse(&self) -> io::Result<()>;
    /// The read mapping over the whole file, where there is one. `None` makes reads use
    /// positioned reads (the simulation has no mapping).
    fn map(&self) -> io::Result<Option<Mmap>>;
    /// Clones a 4096-byte aligned range from `src` without copying payload bytes where
    /// supported (`ioctl(FICLONERANGE)` on Linux XFS/Btrfs); returns `Unsupported` elsewhere.
    fn clone_range_from(&self, _src: &Self, _src_offset: u64, _len: u64, _dest_offset: u64) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "clone_range_from unsupported"))
    }
}

/// Where a volume's files live: the disk, or the simulation's in-memory files.
pub trait VolFs: Send + Sync + 'static {
    type File: VolFile;
    /// Opens `path` for reading and writing, creating it when `create` is set.
    fn open(&self, path: &Path, kind: FileKind, create: bool) -> io::Result<Arc<Self::File>>;
    fn exists(&self, path: &Path) -> bool;
    fn remove(&self, path: &Path) -> io::Result<()>;
    /// The files directly in `dir`, in path order; empty when `dir` does not exist.
    fn list(&self, dir: &Path) -> io::Result<Vec<std::path::PathBuf>>;
}

/// A volume file on disk, with the handles the volume code keeps today:
/// - `main`: the handle writes go through (direct I/O for the data file when enabled);
/// - `sync`: a second open used only to flush. On Windows a write waits for a flush made
///   through the same handle, so flushing through `main` stalled every writer
///   (`measurements/2026-11-14-sync-probe.md`); a flush covers the file whichever handle asks;
/// - `cached`: a read-only open without direct I/O, for reads through the page cache (#155).
pub struct DiskFile {
    main: AlignedFile,
    sync: File,
    cached: File,
}

impl DiskFile {
    pub fn open(path: &Path, kind: FileKind, create: bool) -> io::Result<Self> {
        if !create && !path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound, format!("{} does not exist", path.display())));
        }
        let main = match kind {
            FileKind::Data => AlignedFile::open_direct(path, false)?,
            FileKind::Journal | FileKind::Segment => AlignedFile::open_buffered(path)?,
        };
        // Second opens, not `try_clone`: a clone shares the handle (see `sync` above).
        let sync = OpenOptions::new().write(true).open(path)?;
        let cached = OpenOptions::new().read(true).open(path)?;
        Ok(DiskFile { main, sync, cached })
    }

    /// The handle writes go through.
    pub fn main(&self) -> &AlignedFile {
        &self.main
    }

    /// The same as the trait's, callable without importing `VolFile`: code that held the
    /// volume's `AlignedFile` before the seam (tests among it) keeps working unchanged.
    pub fn write_aligned_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.main.write_aligned_at(offset, buf)
    }

    pub fn read_aligned_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.main.read_aligned_at(offset, buf)
    }
}

impl VolFile for DiskFile {
    fn write_aligned_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        DiskFile::write_aligned_at(self, offset, buf)
    }
    fn read_aligned_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        DiskFile::read_aligned_at(self, offset, buf)
    }
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        crate::direct_io::write_all_at(self.main.file(), offset, buf)
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        crate::direct_io::read_exact_at(&self.cached, offset, buf)
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.main.metadata()?.len())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.main.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.sync.sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        self.main.sync_all()
    }
    fn make_sparse(&self) -> io::Result<()> {
        self.main.make_sparse()
    }
    fn map(&self) -> io::Result<Option<Mmap>> {
        // SAFETY: as everywhere volumes map their file: AwanFS only appends to a volume and
        // never truncates below a mapped length while a mapping is in use.
        Ok(Some(unsafe { Mmap::map(self.main.file()) }?))
    }
    fn clone_range_from(&self, src: &Self, src_offset: u64, len: u64, dest_offset: u64) -> io::Result<()> {
        self.main.clone_range_from(&src.main, src_offset, len, dest_offset)
    }
}

/// The real file system.
#[derive(Clone, Copy, Debug, Default)]
pub struct DiskFs;

impl VolFs for DiskFs {
    type File = DiskFile;
    fn open(&self, path: &Path, kind: FileKind, create: bool) -> io::Result<Arc<DiskFile>> {
        Ok(Arc::new(DiskFile::open(path, kind, create)?))
    }
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
    fn list(&self, dir: &Path) -> io::Result<Vec<std::path::PathBuf>> {
        let rd = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for e in rd {
            let e = e?;
            if e.file_type()?.is_file() {
                out.push(e.path());
            }
        }
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> std::path::PathBuf {
        // No wall clock outside clock.rs (clock_and_dates_test): a counter keeps names apart.
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!("awanfs_vfile_{}_{}_{}", name, std::process::id(), n));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn a_disk_file_reads_back_what_each_handle_wrote() {
        let d = dir("disk");
        let p = d.join("volume_0001.dat");
        assert!(DiskFs.open(&p, FileKind::Data, false).is_err(), "no file, no create");
        let f = DiskFs.open(&p, FileKind::Data, true).unwrap();
        let sector = vec![7u8; 4096];
        f.write_aligned_at(0, &sector).unwrap();
        f.sync_data().unwrap();
        let mut a = vec![0u8; 4096];
        f.read_aligned_at(0, &mut a).unwrap();
        assert_eq!(a, sector);
        let mut b = vec![0u8; 10];
        f.read_at(5, &mut b).unwrap();
        assert_eq!(b, vec![7u8; 10], "the page-cache handle sees the write");
        assert_eq!(f.len().unwrap(), 4096);
        assert!(f.map().unwrap().is_some());

        let j = DiskFs.open(&d.join("volume_0001.idx"), FileKind::Journal, true).unwrap();
        j.write_at(0, b"record-one").unwrap();
        j.write_at(10, b"record-two").unwrap();
        j.sync_data().unwrap();
        let mut r = vec![0u8; 20];
        j.read_at(0, &mut r).unwrap();
        assert_eq!(&r, b"record-onerecord-two");
        j.set_len(10).unwrap();
        assert_eq!(j.len().unwrap(), 10);
        let mut names: Vec<_> = DiskFs.list(&d).unwrap().into_iter().map(|p| p.file_name().unwrap().to_owned()).collect();
        names.sort();
        assert_eq!(names, vec!["volume_0001.dat", "volume_0001.idx"]);
        assert!(DiskFs.list(&d.join("nothing-here")).unwrap().is_empty());
        std::fs::remove_dir_all(&d).ok();
    }
}
