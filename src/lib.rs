use proc_macro::TokenStream;
use proc_macro2::{Ident, Literal, Span};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Attribute, Expr, Field, Fields, GenericArgument, Item, LitStr, PathArguments, Result, Token,
    Type, parse_macro_input,
};

#[derive(Debug)]
struct EntityAttr {
    table_name: Option<String>,
}

impl Default for EntityAttr {
    fn default() -> Self {
        Self { table_name: None }
    }
}

impl Parse for EntityAttr {
    fn parse(input: ParseStream) -> Result<Self> {
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

    let mut struct_item = match item.clone() {
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

    let mut field_name = "id".to_string();
    match &mut struct_item.fields {
        Fields::Named(fields_named) => {
            for field in &mut fields_named.named {
                let attrs = &field.attrs;
                let mut entity_field_attr_opt = None;
                for attr in attrs {
                    entity_field_attr_opt = EntityFieldAttr::from_attrs(attr).unwrap();
                    if let Some(ref entity_field_attr) = entity_field_attr_opt {
                        if entity_field_attr.primary_key {
                            field_name = if let Some(name) = entity_field_attr.name.clone() {
                                name
                            } else {
                                field.ident.clone().unwrap().to_string()
                            };
                        }
                    }
                    break;
                }
                add_sqlx_attr(field, entity_field_attr_opt);
            }
        }
        _ => {}
    };

    let field_name_lit = Literal::string(field_name.as_str());

    let result = quote! {
        #struct_item

        impl batis4sqlx::Entity for #struct_name {
            fn table_name() -> &'static str {
                #table_name
            }
            fn primary_key<'b>() -> batis4sqlx::LambdaField<'b> {
                batis4sqlx::LambdaField::new(#field_name_lit)
            }
        }
    }
    .into();
    result
}

#[derive(Debug, Default)]
struct EntityFieldAttr {
    pub primary_key: bool,
    pub skip: bool,
    pub name: Option<String>,
}

impl EntityFieldAttr {
    pub fn from_attrs(attr: &Attribute) -> Result<Option<Self>> {
        if attr.path().is_ident("entity_field") {
            return Ok(Some(attr.parse_args_with(EntityFieldAttr::parse)?));
        }
        Ok(None)
    }
}

impl Parse for EntityFieldAttr {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut result = EntityFieldAttr::default();

        while !input.is_empty() {
            let ident: Ident = input.parse()?;

            if ident == "primary_key" {
                result.primary_key = true;
            } else if ident == "skip" {
                result.skip = true;
            } else if ident == "name" {
                input.parse::<Token![=]>()?;
                let lit: LitStr = input.parse()?;
                result.name = Some(lit.value());
            } else {
                return Err(syn::Error::new(
                    ident.span(),
                    format!("未知的 entity_field 属性: {}", ident),
                ));
            }

            // 处理逗号
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(result)
    }
}

#[proc_macro_derive(Entity, attributes(entity_field))]
pub fn entity_field(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as Item);

    let struct_item = match &item {
        Item::Struct(s) => s,
        _ => {
            return syn::Error::new(item.span(), "#[derive[Entity]] only use struct")
                .to_compile_error()
                .into();
        }
    };

    let struct_name = &struct_item.ident;
    let field_atts = match &struct_item.fields {
        Fields::Named(fields_named) => fields_named
            .named
            .iter()
            .map(|f| {
                // 字段名称
                let field_name = f.ident.clone().unwrap();
                // 字段类型
                let field_type = &f.ty;
                // 字段上的宏
                let atts = &f.attrs;
                (field_name, field_type, atts)
            })
            .collect(),
        _ => vec![],
    };
    let mut field_keys = vec![];
    let mut get_field_value = vec![];
    let mut lambda_fields = vec![];
    let mut set_primary_key = vec![];
    'outside: for (field, field_type, attrs) in field_atts {
        let field_name = field.to_string();
        let field_ident = Ident::new(&field_name, Span::call_site());
        let func_ident = Ident::new(&format!("{field_name}_field"), Span::call_site());
        let mut field_name_alis_lit = Literal::string(field.to_string().as_str());
        for attr in attrs {
            if let Some(entity_field_attr) = EntityFieldAttr::from_attrs(attr).unwrap() {
                if entity_field_attr.skip {
                    continue 'outside;
                }
                if let Some(name) = entity_field_attr.name {
                    field_name_alis_lit = Literal::string(&name);
                }
                if entity_field_attr.primary_key {
                    if let Some(result) = parse_u64_or_option_u64(field_type) {
                        if result {
                            set_primary_key.push(quote! { self.#field_ident = Some(id) });
                        } else {
                            set_primary_key.push(quote! { self.#field_ident = id });
                        }
                    }
                }
            }
        }
        lambda_fields.push(quote! {
            pub fn #func_ident() -> batis4sqlx::LambdaField<'b> {
                batis4sqlx::LambdaField::new(#field_name_alis_lit)
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
    let result = quote! {
        impl<'b> #struct_name {
            #(#lambda_fields)*
        }

        impl #struct_name {
            pub fn set_primary_key(&mut self, id: u64) {
                #(#set_primary_key)*
            }
        }
    }
    .into();
    result
}

#[derive(Debug, Default)]
struct RepositoryAttr {
    db_type: String,
    entity_path: String,
}

impl Parse for RepositoryAttr {
    fn parse(input: ParseStream) -> Result<Self> {
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
                pub async fn save(&self, vo: &mut #entity_path_ident) -> batis4sqlx::Result<u64> {
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
                    vo.set_primary_key(result.last_insert_id());
                    Ok(result.rows_affected())
                }

                pub async fn save_batch(&self, vec: &Vec<#entity_path_ident>, batch_size: Option<u64>) -> batis4sqlx::Result<u64> {
                    let vec_len = vec.len();
                    let batch_size = if let Some(batch_size) = batch_size {
                        batch_size
                    } else {
                        if vec_len > 1000 { 1000 } else { vec_len as u64 }
                    };
                    let mut insert_sql_prefix = format!(
                        "INSERT INTO {} (",
                        <#entity_path_ident as batis4sqlx::Entity>::table_name()
                    );
                    let lambda_fields = #entity_path_ident::field_keys();
                    let mut values = vec![];
                    for vo in vec {
                        let mut sql_values = vec![];
                        for field in &lambda_fields {
                            let sql_value = vo.get_field_value(field);
                            match sql_value {
                                batis4sqlx::wrapper::SqlValue::Null => {
                                    sql_values.push(batis4sqlx::wrapper::SqlValue::Null);
                                }
                                _ => {
                                    sql_values.push(sql_value);
                                }
                            }
                        }
                        values.push(sql_values);
                    }
                    let fields = lambda_fields
                        .iter()
                        .map(|field| (**field).to_string())
                        .collect::<Vec<String>>()
                        .join(",");
                    insert_sql_prefix.push_str(&fields);
                    insert_sql_prefix.push_str(") VALUES ");
                    let mut index = 0;
                    let mut rows = 0;
                    loop {
                        let mut insert_sql = insert_sql_prefix.clone();
                        let mut sql_values_bind = vec![];
                        for i in index..vec_len {
                            let sql_values = &values[i];
                            insert_sql.push_str("(");
                            let sql_values = sql_values
                                .iter()
                                .map(|sql_value| {
                                    sql_values_bind.push(sql_value.clone());
                                    "?".to_string()
                                })
                                .collect::<Vec<String>>()
                                .join(",");
                            insert_sql.push_str(&sql_values);
                            insert_sql.push_str("),");
                            index += 1;
                            if batch_size % index as u64 == 0 {
                                break;
                            }
                        }
                        if sql_values_bind.is_empty() {
                            break;
                        }
                        insert_sql.pop();
                        let insert = sqlx::query(&insert_sql);
                        let result = batis4sqlx::repository::bind_query(insert, &sql_values_bind)
                            .execute(self.borrow_db())
                            .await;
                        if let Ok(result) = result {
                            rows += result.rows_affected();
                        } else {
                            return Err(result.err().unwrap());
                        }
                        if index >= vec_len {
                            break;
                        }
                    }
                    Ok(rows)
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
                    let primary_key_lambda = batis4sqlx::LambdaField::new(*primary_key);
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

fn parse_u64_or_option_u64(ty: &Type) -> Option<bool> {
    if is_u64(ty) {
        return Some(false);
    }

    if let Type::Path(tp) = ty
        && tp.qself.is_none()
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "Option"
        && let PathArguments::AngleBracketed(args) = &seg.arguments
        && args.args.len() == 1
        && let Some(GenericArgument::Type(inner_ty)) = args.args.first()
        && is_u64(inner_ty)
    {
        return Some(true);
    }

    None
}

fn is_u64(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Path(tp)
            if tp.qself.is_none()
            && tp.path.is_ident("u64")
    )
}

#[derive(Debug, Default)]
struct SqlxAttr {
    default: bool,
    skip: bool,
    rename: Option<String>,
}

impl SqlxAttr {
    pub fn from_attrs(attr: &Attribute) -> Result<Option<Self>> {
        if attr.path().is_ident("sqlx") {
            return Ok(Some(attr.parse_args_with(SqlxAttr::parse)?));
        }
        Ok(None)
    }
}

impl Parse for SqlxAttr {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut result = SqlxAttr::default();

        while !input.is_empty() {
            let ident: Ident = input.parse()?;

            if ident == "default" {
                result.default = true;
            } else if ident == "skip" {
                result.skip = true;
            } else if ident == "rename" {
                input.parse::<Token![=]>()?;
                let lit: LitStr = input.parse()?;
                result.rename = Some(lit.value());
            } else {
            }

            // 处理逗号
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(result)
    }
}

fn add_sqlx_attr(field: &mut Field, entity_field_attr_opt: Option<EntityFieldAttr>) {
    let mut sqlx_skip = false;
    let mut sqlx_default = false;
    let mut sqlx_rename = None;
    for attr in &field.attrs {
        if let Some(sqlx_attr) = SqlxAttr::from_attrs(attr).unwrap() {
            sqlx_skip = sqlx_attr.skip;
            sqlx_default = sqlx_attr.default;
            sqlx_rename = sqlx_attr.rename;
        }
    }
    let mut meta_tokens = vec![];
    if let Some(entity_field_attr) = entity_field_attr_opt {
        if entity_field_attr.skip {
            if sqlx_skip {
                return;
            } else {
                meta_tokens.push(quote!(skip));
            }
        } else {
            if !sqlx_default {
                meta_tokens.push(quote!(default));
            }
            if sqlx_rename.is_none()
                && let Some(ref name) = entity_field_attr.name
            {
                meta_tokens.push(quote!(rename = #name));
            }
        }
    } else {
        if !sqlx_skip && !sqlx_default {
            meta_tokens.push(quote!(default));
        }
    }

    if meta_tokens.is_empty() {
        return;
    }

    let sqlx_attr = syn::parse_quote! {
        #[sqlx( #(#meta_tokens),* )]
    };

    field.attrs.insert(0, sqlx_attr);
}
