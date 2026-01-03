use proc_macro::TokenStream;
use proc_macro2::{Ident, Literal, Span};
use quote::quote;
use std::collections::HashSet;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, Fields, Item, Token, parse_macro_input};

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
    let fields = match &struct_item.fields {
        Fields::Named(fields_named) => fields_named
            .named
            .iter()
            .map(|f| f.ident.clone().unwrap())
            .collect(),
        _ => vec![],
    };
    for field in fields {
        let field_name = field.to_string();
        let field_lit = Literal::string(&field_name);
        let func_ident = Ident::new(&format!("{field_name}_field"), Span::call_site());
        lambda_fields.push(quote! {
            pub fn #func_ident() -> batis4sqlx::LambdaField<'b> {batis4sqlx::LambdaField::new(#field_lit)}
        });
    }

    let expanded = quote! {
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
    .into();
    println!("{}", expanded);
    expanded
}

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
                    if let Expr::Lit(expr_lit) = &meta.value {
                        if let syn::Lit::Str(lit_str) = &expr_lit.lit {
                            attr.table_name = Some(lit_str.value());
                        }
                    }
                }
                "primary_key" => {
                    if let Expr::Lit(expr_lit) = &meta.value {
                        if let syn::Lit::Str(lit_str) = &expr_lit.lit {
                            attr.primary_key = Some(lit_str.value());
                        }
                    }
                }
                "ignore_field" => {
                    if let Expr::Array(expr_array) = &meta.value {
                        attr.ignore_field = expr_array
                            .elems
                            .iter()
                            .filter_map(|elem| {
                                if let Expr::Lit(expr_lit) = elem {
                                    if let syn::Lit::Str(lit_str) = &expr_lit.lit {
                                        return Some(lit_str.value());
                                    }
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
