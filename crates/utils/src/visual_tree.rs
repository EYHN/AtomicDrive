pub fn visual_tree_from_entities<
    'a,
    Entities: Iterator<Item = (&'a str, Content)>,
    Content: std::fmt::Display,
>(
    entities: Entities,
    pat: &str,
) -> String {
    let mut tree = String::new();

    let mut entities = entities.collect::<Vec<_>>();
    entities.sort_by_key(|v| v.0);

    // let mut stack = vec![];
    for (i, entity) in entities.iter().enumerate() {
        let next = entities.get(i + 1);
        let (path, content) = entity;
        let mut level = 0;
        let parts = path.split(pat).filter(|part| part.len() > 0);
        for part in parts {
            if level == 0 {
                tree.push_str(part);
                tree.push_str("\n");
            } else {
                for _ in 0..level {
                    tree.push_str("| ")
                }
                if next.is_some() {
                    tree.push_str("├");
                } else {
                    tree.push_str("└");
                }
                tree.push_str(part);
                tree.push_str("\n");
            }
            level += 1;
        }
    }

    return tree;
}

#[cfg(test)]
mod tests {
    use crate::visual_tree_from_entities;

    #[test]
    fn test() {
        println!(
            "{}",
            visual_tree_from_entities([("/ccc/ddd", "hello")].into_iter(), "/")
        );
    }
}
