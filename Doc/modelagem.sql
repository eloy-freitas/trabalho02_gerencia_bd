create table if not exists modelagem.d_produto(
	sk_produto bigint primary key not null,
	cd_produto bigint not null,
	ds_produto varchar(80) not null,
	qtd_produto_embalagem varchar(12) not null
);

create table if not exists modelagem.d_estabelecimento(
	sk_estabelecimento bigint primary key not null,
	cd_estabelecimento bigint not null,
	cd_filial_estabelecimento bigint not null,
	ds_endereco_estabelecimento varchar(40) not null,
	ds_endereco_complemento varchar(200) not null,
	nu_endereco varchar(5) not null,
	nu_telefone varchar(15) not null,
	no_rede_estabelecimento varchar(15) not null,
	ds_razao_social varchar(50) not null
);

create table if not exists modelagem.d_bairro(
	sk_bairro bigint primary key not null,
	no_bairro varchar(20) not null,
	no_cidade varchar(30) not null
);

create table if not exists modelagem.d_categoria(
	sk_categoria bigint primary key not null,
	cd_categoria bigint not null,
	ds_categoria varchar(60) not null
);

create table if not exists modelagem.d_unidade_medida(
	sk_unidade_medida bigint primary key not null,
	cd_unidade_medida bigint not null,
	ds_unidade_medida varchar(30),
	ds_unidade_medida_sigla varchar(2)
);

create table if not exists modelagem.d_embalagem(
	sk_embalagem bigint primary key not null,
	cd_embalagem bigint not null,
	ds_embalagem varchar(30) not null,
	ds_embalagem_sigla varchar(3)
);

create table if not exists modelagem.f_pesquisa(
	sk_produto bigint not null,
	sk_estabelecimento bigint not null,
	sk_bairro bigint not null,
	sk_categoria bigint not null,
	sk_unidade_medida bigint not null,
	sk_embalagem bigint not null,
	vl_preco_pesquisado float not null,
	dt_pesquisa date not null,
	foreign key (sk_produto) references modelagem.d_produto (sk_produto),
	foreign key (sk_estabelecimento) references modelagem.d_estabelecimento (sk_estabelecimento),
	foreign key (sk_bairro) references modelagem.d_bairro (sk_bairro),
	foreign key (sk_categoria) references modelagem.d_categoria (sk_categoria),
	foreign key (sk_unidade_medida) references modelagem.d_unidade_medida (sk_unidade_medida),
	foreign key (sk_embalagem) references modelagem.d_embalagem (sk_embalagem)
);





