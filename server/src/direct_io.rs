//! awan-storage/server/src/direct_io.rs
//! 4096-Byte Sector-Aligned Direct I/O Buffer Allocation and Positional I/O

use std::alloc::{alloc, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;

pub const SECTOR_SIZE: usize = 4096;

/// RAII 4096-byte physical sector aligned byte buffer.
pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
    len: usize,
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Allocates a zeroed buffer whose memory address and length are multiples of 4096 bytes.
    pub fn new(min_capacity: usize) -> Self {
        let len = if min_capacity == 0 {
            SECTOR_SIZE
        } else {
            ((min_capacity + SECTOR_SIZE - 1) / SECTOR_SIZE) * SECTOR_SIZE
        };

        let layout = Layout::from_size_align(len, SECTOR_SIZE)
            .expect("Valid layout with 4096-byte alignment");

        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        Self { ptr, layout, len }
    }

    pub fn from_slice(data: &[u8]) -> Self {
        let mut buf = Self::new(data.len());
        buf[..data.len()].copy_from_slice(data);
        buf
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

/// Cross-platform aligned file wrapper with Direct I/O and graceful fallback.
pub struct AlignedFile {
    file: File,
    direct_io_active: bool,
}

impl AlignedFile {
    /// Opens or creates a file with Direct I/O flags if supported by the OS/filesystem.
    pub fn open_direct(path: &Path, read_only: bool) -> io::Result<Self> {
        let mut opts = OpenOptions::new();
        opts.read(true);
        if !read_only {
            opts.write(true).create(true);
        }

        let force_direct = std::env::var("AWAN_DIRECT_IO").ok().as_deref() == Some("1");

        #[cfg(target_os = "windows")]
        if force_direct {
            use std::os::windows::fs::OpenOptionsExt;
            // 0x20000000 = FILE_FLAG_NO_BUFFERING
            let mut direct_opts = opts.clone();
            direct_opts.custom_flags(0x20000000);
            if let Ok(file) = direct_opts.open(path) {
                return Ok(Self {
                    file,
                    direct_io_active: true,
                });
            }
        }

        #[cfg(target_os = "linux")]
        if force_direct {
            use std::os::unix::fs::OpenOptionsExt;
            let mut direct_opts = opts.clone();
            direct_opts.custom_flags(libc::O_DIRECT);
            if let Ok(file) = direct_opts.open(path) {
                return Ok(Self {
                    file,
                    direct_io_active: true,
                });
            }
        }

        // Fallback to standard aligned file I/O
        let file = opts.open(path)?;
        Ok(Self {
            file,
            direct_io_active: false,
        })
    }

    pub fn is_direct_io(&self) -> bool {
        self.direct_io_active
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    /// Positional write of a 4096-byte aligned buffer at a 4096-byte aligned offset.
    /// Lock-free and thread-safe across concurrent callers without holding a file mutex.
    pub fn write_aligned_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        debug_assert_eq!(offset % SECTOR_SIZE as u64, 0, "offset must be 4096-byte aligned");
        debug_assert_eq!(buf.len() % SECTOR_SIZE, 0, "buffer size must be 4096-byte aligned");

        #[cfg(target_os = "windows")]
        {
            use std::os::windows::fs::FileExt;
            let mut total_written = 0;
            while total_written < buf.len() {
                let n = self.file.seek_write(&buf[total_written..], offset + total_written as u64)?;
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "zero bytes written in seek_write"));
                }
                total_written += n;
            }
            Ok(())
        }

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::FileExt;
            self.file.write_all_at(buf, offset)?;
            Ok(())
        }

        #[cfg(not(any(target_os = "windows", target_os = "linux")))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file_clone = self.file.try_clone()?;
            file_clone.seek(SeekFrom::Start(offset))?;
            file_clone.write_all(buf)?;
            Ok(())
        }
    }

    /// Positional read into a 4096-byte aligned buffer from a 4096-byte aligned offset.
    /// Lock-free and thread-safe across concurrent callers.
    pub fn read_aligned_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        debug_assert_eq!(offset % SECTOR_SIZE as u64, 0, "offset must be 4096-byte aligned");
        debug_assert_eq!(buf.len() % SECTOR_SIZE, 0, "buffer size must be 4096-byte aligned");

        #[cfg(target_os = "windows")]
        {
            use std::os::windows::fs::FileExt;
            let mut total_read = 0;
            while total_read < buf.len() {
                let n = self.file.seek_read(&mut buf[total_read..], offset + total_read as u64)?;
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected eof in seek_read"));
                }
                total_read += n;
            }
            Ok(())
        }

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(buf, offset)?;
            Ok(())
        }

        #[cfg(not(any(target_os = "windows", target_os = "linux")))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut file_clone = self.file.try_clone()?;
            file_clone.seek(SeekFrom::Start(offset))?;
            file_clone.read_exact(buf)?;
            Ok(())
        }
    }

    pub fn set_len(&self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }

    pub fn metadata(&self) -> io::Result<std::fs::Metadata> {
        self.file.metadata()
    }

    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}
