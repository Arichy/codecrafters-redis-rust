use std::ops::Index;

pub fn index<T>(slice: &[T], start: i32, end: i32) -> &[T] {
    let len = slice.len();

    // let mut start = start;
    // let mut end = end.min(len - 1);
    // if start < 0 {
    //     start = start + len;
    //     if start < 0 {
    //         start = 0;
    //     }
    // }

    // if end < 0 {
    //     end = end + len;
    //     if end < 0 {
    //         end = 0;
    //     }
    // }

    // if start > end || start >= len as i32 {
    //     return &[];
    // }

    match normalize_index(len, start, end) {
        Some((start, end)) => &slice[start as usize..=end as usize],
        None => &[],
    }

    // &slice[start as usize..=end as usize]
}

pub fn normalize_index(len: usize, start: i32, end: i32) -> Option<(usize, usize)> {
    let len = len as i32;
    let mut start = start;
    let mut end = end.min(len - 1);
    if start < 0 {
        start = start + len;
        if start < 0 {
            start = 0;
        }
    }

    if end < 0 {
        end = end + len;
        if end < 0 {
            end = 0;
        }
    }

    if start > end || start >= len as i32 {
        return None;
    }

    Some((start as usize, end as usize))
}
