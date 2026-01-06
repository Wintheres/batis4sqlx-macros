use proc_macro::TokenStream;
use proc_macro2::{Ident, Literal, Span};
use quote::quote;
use std::collections::HashSet;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, Fields, Item, Token, parse_macro_input};

#[derive(Debug, Default)]
struct EntityAttr {
    table_name: Option<String>,
    primary_key: Option<String>,
    ignore_field: HashSet<String>,
}

impl Parse for EntityAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut attr = EntityAttr::default();
        let args: Punctuated<syn::MetaNameValue, Token![,]> = Punctuated::parse_terminated(input)?;
        for meta in args {
            match meta.path.get_ident().unwrap().to_string().as_str() {
                "table_name" => {
                    if let Expr::Lit(expr_lit) = &meta.value
                        && let syn::Lit::Str(lit_str) = &expr_lit.lit
                    {
                        attr.table_name = Some(lit_str.value());
                    }
                }
                "primary_key" => {
                    if let Expr::Lit(expr_lit) = &meta.value
                        && let syn::Lit::Str(lit_str) = &expr_lit.lit
                    {
                        attr.primary_key = Some(lit_str.value());
                    }
                }
                "ignore_field" => {
                    if let Expr::Array(expr_array) = &meta.value {
                        attr.ignore_field = expr_array
                            .elems
                            .iter()
                            .filter_map(|elem| {
                                if let Expr::Lit(expr_lit) = elem
                                    && let syn::Lit::Str(lit_str) = &expr_lit.lit
                                {
                                    return Some(lit_str.value());
                                }
                                None
                            })
                            .collect();
                    }
                }
                _ => {}
            }
        }
        Ok(attr)
    }
}

#[proc_macro_attribute]
pub fn entity(attr: TokenStream, input: TokenStream) -> TokenStream {
    let entity_attr = syn::parse_macro_input!(attr as EntityAttr);
    let item = parse_macro_input!(input as Item);

    let struct_item = match &item {
        Item::Struct(s) => s,
        _ => {
            return syn::Error::new(item.span(), "#[entity] only use struct")
                .to_compile_error()
                .into();
        }
    };

    let struct_name = &struct_item.ident;
    let table_name = if let Some(table_name) = &entity_attr.table_name {
        table_name.as_str()
    } else {
        &pascal_to_snake(&struct_name.to_string())
    };
    let primary_key = if let Some(primary_key) = &entity_attr.primary_key {
        primary_key.as_str()
    } else {
        "id"
    };

    let mut lambda_fields = vec![];
    let mut field_keys = vec![];
    let mut get_field_value = vec![];
    let field_atts = match &struct_item.fields {
        Fields::Named(fields_named) => fields_named
            .named
            .iter()
            .map(|f| {
                // 字段名称
                let field_name = f.ident.clone().unwrap();
                // 字段类型
                let _field_type = &f.ty;
                // 字段上的宏
                let atts = &f.attrs;
                (field_name, atts)
            })
            .collect(),
        _ => vec![],
    };
    'outside: for (field, atts) in field_atts {
        let field_name = field.to_string();
        let mut has_sqlx_skip = false;
        for att in atts {
            if att.path().is_ident("sqlx") {
                let _ = att.parse_nested_meta(|meta| {
                    if meta.path.is_ident("skip") {
                        has_sqlx_skip = true;
                    }
                    Ok(())
                });
            }
        }
        if has_sqlx_skip || entity_attr.ignore_field.contains(&field_name) {
            continue 'outside;
        }
        let field_lit = Literal::string(&field_name);
        let field_ident = Ident::new(&field_name, Span::call_site());
        let func_ident = Ident::new(&format!("{field_name}_field"), Span::call_site());
        lambda_fields.push(quote! {
            pub fn #func_ident() -> batis4sqlx::LambdaField<'b> {
                batis4sqlx::LambdaField::new(#field_lit)
            }
        });
        field_keys.push(quote! {
            field_keys.push(Self::#func_ident());
        });
        get_field_value.push(quote! {
            k if k == *Self::#func_ident() => {
                if let Some(ref value) = self.#field_ident {
                    value.clone().into()
                } else {
                    batis4sqlx::wrapper::SqlValue::Null
                }
            },
        })
    }
    lambda_fields.push(quote! {
        pub fn field_keys() -> Vec<batis4sqlx::LambdaField<'b>> {
            let mut field_keys = vec![];
            #(#field_keys)*
            field_keys
        }

        pub fn get_field_value(&self, key: &batis4sqlx::LambdaField<'b>) -> batis4sqlx::wrapper::SqlValue {
            match **key {
                #(#get_field_value)*
                &_ => {
                    panic!("unknown field")
                }
            }
        }
    });

    quote! {
        #item

        impl batis4sqlx::Entity for #struct_name {
            fn table_name() -> &'static str {
                #table_name
            }

            fn primary_key() -> &'static str {
                #primary_key
            }
        }

        impl<'b> #struct_name {
            #(#lambda_fields)*
        }
    }
    .into()
}

#[derive(Debug, Default)]
struct RepositoryAttr {
    db_type: String,
    entity_path: String,
}

impl Parse for RepositoryAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut attr = RepositoryAttr::default();
        let args: Punctuated<syn::MetaNameValue, Token![,]> = Punctuated::parse_terminated(input)?;
        for meta in args {
            match meta.path.get_ident().unwrap().to_string().as_str() {
                "db_type" => {
                    if let Expr::Lit(expr_lit) = &meta.value
                        && let syn::Lit::Str(lit_str) = &expr_lit.lit
                    {
                        attr.db_type = lit_str.value();
                    }
                }
                "entity_path" => {
                    if let Expr::Lit(expr_lit) = &meta.value
                        && let syn::Lit::Str(lit_str) = &expr_lit.lit
                    {
                        attr.entity_path = lit_str.value();
                    }
                }
                _ => {}
            }
        }
        Ok(attr)
    }
}

#[proc_macro_attribute]
pub fn repository(attr: TokenStream, input: TokenStream) -> TokenStream {
    let repository_attr = syn::parse_macro_input!(attr as RepositoryAttr);
    let item = parse_macro_input!(input as Item);

    let struct_item = match &item {
        Item::Struct(s) => s,
        _ => {
            return syn::Error::new(item.span(), "#[repository] only use struct")
                .to_compile_error()
                .into();
        }
    };

    let struct_name = &struct_item.ident;
    let entity_path = repository_attr.entity_path.as_str();
    let entity_path_ident = Ident::new(entity_path, Span::call_site());
    let impls = match repository_attr.db_type.to_lowercase().as_str() {
        "mysql" => {
            quote! {
                pub async fn save(&self, vo: &#entity_path_ident) -> batis4sqlx::Result<u64> {
                    let mut insert_sql = format!("INSERT INTO {} (", <#entity_path_ident as batis4sqlx::Entity>::table_name());
                    let mut fields = vec![];
                    let mut values = vec![];
                    for field in #entity_path_ident::field_keys() {
                        let sql_value = vo.get_field_value(&field);
                        match sql_value {
                            batis4sqlx::wrapper::SqlValue::Null => {
                                continue;
                            }
                            _ => {
                                fields.push(field);
                                values.push(sql_value);
                            }
                        }
                    }
                    let fields = fields
                        .iter()
                        .map(|field| (**field).to_string())
                        .collect::<Vec<String>>()
                        .join(",");
                    insert_sql.push_str(&fields);
                    insert_sql.push_str(") VALUES (");
                    {
                        let values = values
                            .iter()
                            .map(|_| "?".to_string())
                            .collect::<Vec<String>>()
                            .join(",");
                        insert_sql.push_str(&values);
                    }
                    insert_sql.push_str(")");
                    let insert = sqlx::query(&insert_sql);
                    let result = batis4sqlx::repository::bind_query(insert, &values)
                        .execute(self.borrow_db())
                        .await?;
                    Ok(result.rows_affected())
                }

                pub async fn update_by_primary_key(&self, vo: &#entity_path_ident) -> batis4sqlx::Result<u64> {
                    let mut update_sql = format!("UPDATE {} SET ", <#entity_path_ident as batis4sqlx::Entity>::table_name());
                    let mut fields = vec![];
                    let mut values = vec![];
                    for field in #entity_path_ident::field_keys() {
                        let sql_value = vo.get_field_value(&field);
                        match sql_value {
                            batis4sqlx::wrapper::SqlValue::Null => {
                                continue;
                            }
                            _ => {
                                fields.push(field);
                                values.push(sql_value);
                            }
                        }
                    }
                    let fields = fields
                        .iter()
                        .map(|field| format!("{} = ?", **field))
                        .collect::<Vec<String>>()
                        .join(", ");
                    update_sql.push_str(&fields);
                    let primary_key = <#entity_path_ident as batis4sqlx::Entity>::primary_key();
                    let primary_key_lambda = batis4sqlx::LambdaField::new(primary_key);
                    values.push(vo.get_field_value(&primary_key_lambda));
                    update_sql.push_str(&format!(" WHERE {} = ? LIMIT 1", primary_key));
                    let update = sqlx::query(&update_sql);
                    let result = batis4sqlx::repository::bind_query(update, &values)
                        .execute(self.borrow_db())
                        .await?;
                    Ok(result.rows_affected())
                }

                pub async fn delete_in_primary_keys<T>(&self, primary_keys: std::collections::HashSet<T>) -> batis4sqlx::Result<u64>
                where
                    T: for<'q> sqlx::Encode<'q, sqlx::MySql> + sqlx::Type<sqlx::MySql>,
                {
                    let placeholders = primary_keys
                        .iter()
                        .map(|_| "?")
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = format!(
                        "DELETE FROM {} WHERE `{}` IN ({placeholders})",
                        <#entity_path_ident as batis4sqlx::Entity>::table_name(),
                        <#entity_path_ident as batis4sqlx::Entity>::primary_key()
                    );
                    let mut delete = sqlx::query(&sql);
                    for id in primary_keys {
                        delete = delete.bind(id);
                    }
                    Ok(delete.execute(self.borrow_db()).await?.rows_affected())
                }
            }
        }
        _ => {
            return syn::Error::new(item.span(), "unknow db type.")
                .to_compile_error()
                .into();
        }
    };

    let expanded = quote! {
        #item

        impl #struct_name {
            #impls
        }
    }
    .into();
    println!("{}", expanded);
    expanded
}

fn pascal_to_snake(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut prev_is_upper = false;
    let mut prev_is_alpha = false;

    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 && (!prev_is_upper || prev_is_alpha) {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
            prev_is_upper = true;
            prev_is_alpha = true;
        } else {
            result.push(ch);
            prev_is_upper = false;
            prev_is_alpha = ch.is_alphabetic();
        }
    }

    result
}
