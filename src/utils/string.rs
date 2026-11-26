//! String utility functions

/// Items that cannot stack in Minecraft/Hypixel SkyBlock (max stack size = 1).
/// When buying these items, the order amount must be capped to available
/// inventory slots since each unit occupies one slot.
const UNSTACKABLE_ITEM_NAMES: &[&str] = &["enchanted book"];

/// Item tag prefixes that indicate unstackable items.
const UNSTACKABLE_TAG_PREFIXES: &[&str] = &["ENCHANTMENT_"];

/// Returns true if the given bazaar item is unstackable (max stack size = 1).
/// Checks both the item name and the optional item tag.
pub fn is_unstackable_item(item_name: &str, item_tag: Option<&str>) -> bool {
    let lower = item_name.to_lowercase();
    if UNSTACKABLE_ITEM_NAMES.iter().any(|&n| lower.contains(n)) {
        return true;
    }
    if let Some(tag) = item_tag {
        if UNSTACKABLE_TAG_PREFIXES
            .iter()
            .any(|&prefix| tag.starts_with(prefix))
        {
            return true;
        }
    }
    false
}

/// Format a number with thousands separators
pub fn format_number_with_separators(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, ch) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(*ch);
    }

    result
}

/// Remove Minecraft formatting codes from text.
///
/// This is the ONE implementation; every other "strip colours" helper in the
/// crate delegates here so the same item name or chat line always cleans to the
/// same string, whichever code path handled it.
///
/// Strips `§` plus the following character, whatever it is: that covers colour
/// and style codes in either case (`§a`, `§A`, `§l`, `§r`) as well as each pair
/// of the `§x§R§R§G§G§B§B` hex form. Also strips the mojibake form `┬º` plus its
/// code character, which is what `§` becomes when UTF-8 text is decoded as
/// CP437 (seen in Windows console copies).
pub fn remove_minecraft_colors(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '§' => {
                chars.next();
            }
            '┬' if chars.peek() == Some(&'º') => {
                chars.next(); // 'º'
                chars.next(); // the code character
            }
            _ => result.push(ch),
        }
    }

    result
}

/// Convert string to title case
pub fn to_title_case(s: &str) -> String {
    s.split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                }
            }
        })
        .collect::<Vec<String>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number_with_separators() {
        assert_eq!(format_number_with_separators(1000), "1,000");
        assert_eq!(format_number_with_separators(1000000), "1,000,000");
        assert_eq!(format_number_with_separators(123), "123");
    }

    #[test]
    fn test_remove_minecraft_colors() {
        assert_eq!(remove_minecraft_colors("§aGreen§r Text"), "Green Text");
        assert_eq!(
            remove_minecraft_colors("§6Buy Item Right Now"),
            "Buy Item Right Now"
        );
    }

    #[test]
    fn remove_minecraft_colors_handles_every_form() {
        // uppercase codes, which the old regex-based helpers missed
        assert_eq!(remove_minecraft_colors("§AGreen§R Text"), "Green Text");
        // hex colour sequence
        assert_eq!(remove_minecraft_colors("§x§f§f§0§0§0§0Red"), "Red");
        // CP437 mojibake of §
        assert_eq!(remove_minecraft_colors("┬º6Gold"), "Gold");
        // a lone ┬ (box drawing) is real text, not a code marker
        assert_eq!(remove_minecraft_colors("a┬b"), "a┬b");
        // trailing § does not panic
        assert_eq!(remove_minecraft_colors("end§"), "end");
    }

    #[test]
    fn test_to_title_case() {
        assert_eq!(to_title_case("hello world"), "Hello World");
        assert_eq!(to_title_case("HELLO WORLD"), "Hello World");
        assert_eq!(to_title_case("hello"), "Hello");
    }

    #[test]
    fn test_is_unstackable_item_enchanted_book_name() {
        assert!(is_unstackable_item("Enchanted Book", None));
        assert!(is_unstackable_item("enchanted book", None));
    }

    #[test]
    fn test_is_unstackable_item_enchantment_tag() {
        assert!(is_unstackable_item(
            "Enchanted Book",
            Some("ENCHANTMENT_ICE_COLD_1")
        ));
        assert!(is_unstackable_item(
            "Enchanted Book",
            Some("ENCHANTMENT_BLAST_PROTECTION_7")
        ));
        // Even with a non-book name, the tag prefix should match
        assert!(is_unstackable_item(
            "Some Item",
            Some("ENCHANTMENT_SHARPNESS_6")
        ));
    }

    #[test]
    fn test_is_unstackable_item_stackable() {
        assert!(!is_unstackable_item("Enchanted Diamond", None));
        assert!(!is_unstackable_item(
            "Enchanted Raw Salmon",
            Some("ENCHANTED_RAW_SALMON")
        ));
        assert!(!is_unstackable_item("Rough Peridot Gemstone", None));
    }
}
