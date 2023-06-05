#[derive(Debug, Default)]
struct TreeNode {
    key: String,
    children: Vec<TreeNode>,
    content: String,
}

pub fn tree_stringify<
    'a,
    Entities: Iterator<Item = (&'a str, Content)>,
    Content: std::fmt::Display,
>(
    entities: Entities,
    pat: &str,
) -> String {
    let mut tree: TreeNode = TreeNode::default();

    let mut entities = entities.collect::<Vec<_>>();
    entities.sort_by_key(|v| v.0);

    // let mut stack = vec![];
    for entity in entities.iter() {
        let (path, content) = entity;
        let parts: Vec<_> = path.split(pat).filter(|part| part.len() > 0).collect();

        let mut current_stack: Vec<usize> = vec![];
        for (i, part) in parts.iter().enumerate() {
            let is_end = i == parts.len() - 1;
            let mut current = &mut tree;
            for i in current_stack.iter() {
                current = &mut current.children[*i]
            }
            if let Some((i, _)) = current
                .children
                .iter_mut()
                .enumerate()
                .find(|(_, c)| c.key == *part)
            {
                current_stack.push(i);
            } else {
                current.children.push(TreeNode {
                    key: part.to_string(),
                    content: if is_end {
                        content.to_string()
                    } else {
                        Default::default()
                    },
                    ..Default::default()
                });
                current_stack.push(current.children.len() - 1);
            }
        }
    }

    fn output_tree_children(mut str: String, tree: TreeNode, prefix: &str, pat: &str) -> String {
        let count = tree.children.len();
        for (i, mut entry) in tree.children.into_iter().enumerate() {
            let mut key = entry.key.clone();
            loop {
                if entry.children.len() == 1 && entry.content.is_empty() {
                    entry = entry.children.remove(0);
                    key = format!("{}{}{}", key, pat, entry.key);
                    continue;
                }
                break;
            }
            let mut new_prefix = prefix.to_string();

            let output_content = if entry.content.is_empty() {
                Default::default()
            } else {
                format!(" [{}]", entry.content)
            };

            if count == i + 1 {
                str.push_str(&format!(
                    "{}└ {}{}\n",
                    new_prefix, key, output_content
                ));
                new_prefix.push_str(" ");
            } else {
                str.push_str(&format!(
                    "{}├ {}{}\n",
                    new_prefix, key, output_content
                ));
                new_prefix.push_str("│");
            }

            if !entry.children.is_empty() {
                str = output_tree_children(str, entry, &new_prefix, pat);
            }
        }

        str
    }

    return output_tree_children(Default::default(), tree, "", pat);
}

#[cfg(test)]
mod tests {
    use crate::tree_stringify;

    #[test]
    fn test() {
        println!(
            "{}",
            tree_stringify(
                [
                    ("/ccc/ddd", "hello"),
                    ("/ccc/eee", "hello1"),
                    ("/aaa/ddd", "hello2"),
                    ("/aaa/ddd/eee/fff/ttt/kkk", "hello4"),
                    ("/i/e", "hello5"),
                ]
                .into_iter(),
                "/"
            )
        );
    }
}
